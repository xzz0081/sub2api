package service

// trajectory_packer.go
// 轨迹打包器（v2，段语义 + 即时打包）：
//
// 旧版（v1）的"5 分钟扫描静默 session 整个打包"已下线：
//   - 触发改为「即时」：collector 每次落盘 call 后看 stop_reason==end_turn 立即调用 tryPackSegment，
//     不再有后台 goroutine 定时扫描。
//   - 段语义：同一 session 内可以打多个 zip（{sid}__001.zip / {sid}__002.zip ...），
//     每段对应"上次打包之后到本次 end_turn"之间的 calls。
//   - 段不合格（如 ≥5 轮门槛没过）→ 不打包、不永久判死，等用户接着问下一个 end_turn，
//     把 1..N 整段一起再判一次（"合并到下段"，方案 A）。
//
// 兼容性：
//   - 旧 zip（叫 {sid}.zip 没段号）保留不动，与新分段 zip 共存。
//   - 旧 pack_state.json 字段（status/reason/zip_file/call_count）一次性迁移到新字段
//     PackedSegments / NextStartCall：v1 已 qualified 的 session NextStartCall 跳到 CallCount，
//     v1 unqualified 的 session NextStartCall=0（方案 A 重新评估，不再判死）。

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/tidwall/gjson"
)

// 打包器开关。其余环境变量（旧的 INTERVAL/SILENCE）已废弃，留着仅做向后兼容不再读取。
const (
	trajectoryPackEnv = "SUB2API_TRAJECTORY_PACK" // "1"/"true" 开启自动打包
)

// 打包器固定常量（v1 的 interval/silence 不再使用）。
const (
	sellableSubDir = "sellable"
	packStateFile  = "pack_state.json"
)

// validator 允许集（normal 档，与 SOP validator.py 对齐）。
var (
	packAllowedModels     = []string{"claude-opus-4-6", "claude-opus-4-7"}
	packAllowedEfforts    = map[string]struct{}{"high": {}, "xhigh": {}, "max": {}}
	packMinAssistantTurns = 5
	packMaxToolErrorRate  = 0.25
)

// packState pack_state.json 的结构：记录每个 session 的判定/已打包结果。
type packState struct {
	UpdatedAt string                      `json:"updated_at"`
	Sessions  map[string]packSessionState `json:"sessions"`
}

// packSessionState 单个 session 的状态。
//
// v2 字段：
//   - PackedSegments：已成功打包的段历史（顺序追加）。
//   - NextStartCall：下一段从该 session 排序后的第几个 call 文件开始（0-based）。
//   - LastCheckedAt：上次评估时刻（用于诊断，不参与判定）。
//
// v1 兼容字段（status/reason/checked_at/zip_file/call_count）只用于读旧文件迁移，
// 写新文件时清空，不再使用。
type packSessionState struct {
	// v2 段语义字段
	PackedSegments []packedSegment `json:"packed_segments,omitempty"`
	NextStartCall  int             `json:"next_start_call"`
	LastCheckedAt  string          `json:"last_checked_at,omitempty"`

	// v1 兼容字段（仅迁移期使用）
	Status    string `json:"status,omitempty"`
	Reason    string `json:"reason,omitempty"`
	CheckedAt string `json:"checked_at,omitempty"`
	ZipFile   string `json:"zip_file,omitempty"`
	CallCount int    `json:"call_count,omitempty"`
}

// packedSegment 一段已打包数据的元信息。
type packedSegment struct {
	SeqNo     int    `json:"seq_no"`     // 段序号 1/2/3...（zip 文件名 zero-pad 到 3 位）
	StartCall int    `json:"start_call"` // 段起始 call 索引（0-based，含）
	EndCall   int    `json:"end_call"`   // 段结束 call 索引（0-based，含）
	ZipFile   string `json:"zip_file"`   // {sid}__001.zip
	PackedAt  string `json:"packed_at"`
	CallCount int    `json:"call_count"`
}

// tryPackSegment 由 collector 在 stop_reason==end_turn 的 call 落盘成功后调用，
// 立即尝试把 [NextStartCall .. 当前最后一个 call] 的窗口判定并打包。
//
// 设计要点：
//   - 整个流程持有 packMu，避免同 sid 并发触发导致段窗口竞争。
//   - validator 不通过 → 什么都不做，等下次 end_turn 再用更长的窗口重新评估（方案 A）。
//   - 打包成功 → state.NextStartCall = lastIdx+1，append PackedSegments；
//     失败时不改 state，下次重试。
func (tc *TrajectoryCollector) tryPackSegment(sid string) {
	if tc == nil || !tc.packEnabled || strings.TrimSpace(sid) == "" {
		return
	}

	tc.packMu.Lock()
	defer tc.packMu.Unlock()

	sessionDir := filepath.Join(tc.dir, "sessions", sanitizePathSegment(sid))
	files := tc.listSessionCallFiles(sessionDir)
	if len(files) == 0 {
		return
	}

	// 加载 state（含 v1 → v2 迁移）。
	state := tc.loadPackStateLocked()
	ss := tc.ensureSessionStateLocked(state, sid, len(files))

	// 段窗口 = [NextStartCall, len(files)-1]。
	startIdx := ss.NextStartCall
	endIdx := len(files) - 1
	if startIdx > endIdx {
		// 没有新 call 可打（理论上 collector 刚落盘了一个，不会进这里；防御性 return）。
		return
	}
	segmentFiles := files[startIdx : endIdx+1]

	// 判定 1：段的最后一个 call 必须 stop_reason==end_turn。
	lastCallData, err := os.ReadFile(segmentFiles[len(segmentFiles)-1])
	if err != nil || !gjson.ValidBytes(lastCallData) {
		return
	}
	lastResp := gjson.GetBytes(lastCallData, "response.response_data")
	if lastResp.Get("stop_reason").String() != "end_turn" {
		return // 非 end_turn 的中间轮，等下个 end_turn 再说
	}

	// 判定 2：跑 validator。
	calls, parsed := tc.parseSegmentCalls(segmentFiles)
	if len(calls) == 0 {
		return
	}
	ok, reason := validateSegment(calls)
	now := time.Now()
	ss.LastCheckedAt = now.UTC().Format(time.RFC3339)
	if !ok {
		// 段不合格 → 方案 A：不打包、不前移 NextStartCall，等下个 end_turn 把更长窗口再判一次。
		state.Sessions[sid] = ss
		state.UpdatedAt = ss.LastCheckedAt
		tc.savePackStateLocked(state)
		logger.LegacyPrintf("service.trajectory",
			"segment unqualified: session=%s window=[%d,%d] reason=%s (will retry on next end_turn)",
			sid, startIdx, endIdx, reason)
		return
	}

	// 判定通过 → 打包。
	seqNo := len(ss.PackedSegments) + 1
	zipFile, perr := tc.packSegment(sid, seqNo, parsed)
	if perr != nil {
		// 打包失败：不动 state，下次重试。
		logger.LegacyPrintf("service.trajectory",
			"pack segment failed: session=%s seq=%d window=[%d,%d] err=%v",
			sid, seqNo, startIdx, endIdx, perr)
		return
	}

	ss.PackedSegments = append(ss.PackedSegments, packedSegment{
		SeqNo:     seqNo,
		StartCall: startIdx,
		EndCall:   endIdx,
		ZipFile:   filepath.Base(zipFile),
		PackedAt:  now.UTC().Format(time.RFC3339),
		CallCount: endIdx - startIdx + 1,
	})
	ss.NextStartCall = endIdx + 1
	state.Sessions[sid] = ss
	state.UpdatedAt = ss.LastCheckedAt
	tc.savePackStateLocked(state)

	tc.statSellable.Add(1)
	tc.lastPackAt.Store(now.Unix())
	logger.LegacyPrintf("service.trajectory",
		"segment packed (sellable): session=%s seq=%d window=[%d,%d] calls=%d zip=%s",
		sid, seqNo, startIdx, endIdx, endIdx-startIdx+1, filepath.Base(zipFile))
}

// listSessionCallFiles 列出 session 目录下所有 *.json，按文件名（即时间戳）排序。
func (tc *TrajectoryCollector) listSessionCallFiles(sessionDir string) []string {
	files, _ := filepath.Glob(filepath.Join(sessionDir, "*.json"))
	sort.Strings(files)
	return files
}

// parseSegmentCalls 读取并解析段内 call 文件，返回 (gjson 切片用于 validator, 详细 entry 用于打 zip)。
// 解析失败的文件跳过。
func (tc *TrajectoryCollector) parseSegmentCalls(files []string) ([]gjson.Result, []packCallEntry) {
	calls := make([]gjson.Result, 0, len(files))
	entries := make([]packCallEntry, 0, len(files))
	for _, fp := range files {
		data, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		if !gjson.ValidBytes(data) {
			continue
		}
		// 紧凑化：把格式化 JSON 压成单行，供 all_calls.jsonl 与 sha256 使用。
		var compactBuf bytes.Buffer
		if err := json.Compact(&compactBuf, data); err != nil {
			continue
		}
		compact := compactBuf.Bytes()
		sum := sha256.Sum256(compact)
		root := gjson.ParseBytes(data)
		calls = append(calls, root)
		entries = append(entries, packCallEntry{
			fileName:    filepath.Base(fp),
			sessionID:   root.Get("session_id").String(),
			requestID:   root.Get("request_id").String(),
			timestamp:   root.Get("timestamp").String(),
			model:       root.Get("request.model").String(),
			effort:      root.Get("thinking_effort").String(),
			prettyBytes: data,
			compactJSON: append([]byte{}, compact...),
			sha256Hex:   hex.EncodeToString(sum[:]),
		})
	}
	return calls, entries
}

// ensureSessionStateLocked 取/创建 sid 的 state，并按需做 v1→v2 迁移。
// 调用方必须持有 packMu。
//
// 迁移规则：
//   - 新 sid（state 里完全不存在）→ NextStartCall=0，PackedSegments=空。
//   - v1 已写过该 sid（含旧 status/call_count 字段）：
//     - status=="qualified"：v1 已经把整个 session 打成 {sid}.zip 了 → NextStartCall=CallCount
//       （跳过历史，不与旧 zip 内容重复）。
//     - 其它（unqualified / 空）：NextStartCall=0（方案 A 重新评估）。
//   - 迁移完后清掉 v1 字段，下次写盘就只有 v2 字段了。
//
// totalCalls 用于补救异常情况：如果 v1 CallCount 比当前磁盘文件数还大（极端情况），夹紧到 totalCalls。
func (tc *TrajectoryCollector) ensureSessionStateLocked(state *packState, sid string, totalCalls int) packSessionState {
	ss, exists := state.Sessions[sid]
	if !exists {
		// v2 时代全新 sid。
		ss = packSessionState{PackedSegments: []packedSegment{}, NextStartCall: 0}
		state.Sessions[sid] = ss
		return ss
	}
	// 已有记录：检查是否需要 v1→v2 迁移（PackedSegments==nil 且有 v1 字段）。
	migrated := false
	if ss.PackedSegments == nil {
		if ss.Status == "qualified" && ss.CallCount > 0 {
			ss.NextStartCall = ss.CallCount
		}
		// unqualified / 其它：NextStartCall 默认 0，方案 A 重新评估。
		ss.PackedSegments = []packedSegment{}
		ss.Status = ""
		ss.Reason = ""
		ss.CheckedAt = ""
		ss.ZipFile = ""
		ss.CallCount = 0
		migrated = true
	}
	if ss.NextStartCall > totalCalls {
		ss.NextStartCall = totalCalls
		migrated = true
	}
	if migrated {
		state.Sessions[sid] = ss
	}
	return ss
}

// nonEmptyField 判断一个 gjson 字段是否存在且非空（兼容 string / array / object）。
func nonEmptyField(r gjson.Result) bool {
	if !r.Exists() {
		return false
	}
	if r.Type == gjson.Null {
		return false
	}
	if r.Type == gjson.String {
		return strings.TrimSpace(r.String()) != ""
	}
	raw := strings.TrimSpace(r.Raw)
	return raw != "" && raw != "[]" && raw != "{}" && raw != "null"
}

// iterContentBlocks 遍历一条 message 的 content：若为 array 则逐个 block 回调，string content 无 block 跳过。
func iterContentBlocks(content gjson.Result, fn func(block gjson.Result)) {
	if content.IsArray() {
		for _, b := range content.Array() {
			fn(b)
		}
	}
}

// validateSegment 对一个段（按时间序的若干 calls）跑 normal 档质检。
// 取段内最长 messages 的 call 作为 trajectory（API 设计上=最后一次 call，此处仍按最长筛以防丢轮）。
//
// 与 v1 validateSession 的差别：
//   - 输入直接是给定段的 calls 切片，不再扫整个 session 目录。
//   - 判定逻辑（≥5 assistant 轮 / model / effort / thinking / tool_error / 末轮 end_turn 含 text）不变。
func validateSegment(calls []gjson.Result) (bool, string) {
	if len(calls) == 0 {
		return false, "段为空"
	}

	// 取 messages 最长的 call 作为 trajectory。
	var longest gjson.Result
	maxLen := -1
	for _, c := range calls {
		n := len(c.Get("request.messages").Array())
		if n > maxLen {
			maxLen = n
			longest = c
		}
	}

	req := longest.Get("request")
	resp := longest.Get("response.response_data")

	// ---- 格式级 ----
	effort := strings.ToLower(strings.TrimSpace(longest.Get("thinking_effort").String()))
	if _, ok := packAllowedEfforts[effort]; !ok {
		return false, fmt.Sprintf("effort=%s 不在允许集", effort)
	}
	model := req.Get("model").String()
	matchedModel := false
	for _, m := range packAllowedModels {
		if strings.HasPrefix(model, m) {
			matchedModel = true
			break
		}
	}
	if !matchedModel {
		return false, fmt.Sprintf("model=%s 不在允许集", model)
	}
	if !nonEmptyField(req.Get("system")) {
		return false, "system 为空"
	}
	tools := req.Get("tools")
	if !tools.IsArray() || len(tools.Array()) == 0 {
		return false, "tools 缺失"
	}
	msgs := req.Get("messages").Array()
	if len(msgs) == 0 || msgs[0].Get("role").String() != "user" {
		return false, "messages[0] 不是 user"
	}

	// ---- 质量级（基于最长 trajectory）----
	// 收集所有 assistant 轮的 content：messages 里的 assistant 历史 + 本次 response。
	asstContents := make([]gjson.Result, 0, len(msgs))
	for _, m := range msgs {
		if m.Get("role").String() == "assistant" {
			asstContents = append(asstContents, m.Get("content"))
		}
	}
	asstContents = append(asstContents, resp.Get("content"))
	nAsst := len(asstContents)
	if nAsst < packMinAssistantTurns {
		return false, fmt.Sprintf("assistant 轮数 %d < %d", nAsst, packMinAssistantTurns)
	}

	// 末轮完整性（致命项）：stop_reason 必须 end_turn，且有非空 text 总结。
	stop := resp.Get("stop_reason").String()
	if stop != "end_turn" {
		return false, fmt.Sprintf("末轮 stop_reason=%s(必须 end_turn)", stop)
	}
	hasText := false
	iterContentBlocks(resp.Get("content"), func(b gjson.Result) {
		if b.Get("type").String() == "text" && strings.TrimSpace(b.Get("text").String()) != "" {
			hasText = true
		}
	})
	if !hasText {
		return false, "末轮无非空 text 总结"
	}

	// thinking 与 tool_use schema 检查（遍历所有 assistant 轮）。
	toolNames := make(map[string]struct{}, len(tools.Array()))
	for _, t := range tools.Array() {
		toolNames[t.Get("name").String()] = struct{}{}
	}
	hasThinking := false
	badTool := ""
	for _, cont := range asstContents {
		iterContentBlocks(cont, func(b gjson.Result) {
			switch b.Get("type").String() {
			case "thinking":
				if strings.TrimSpace(b.Get("thinking").String()) != "" {
					hasThinking = true
				}
			case "tool_use":
				if _, ok := toolNames[b.Get("name").String()]; !ok {
					badTool = b.Get("name").String()
				}
			}
		})
	}
	if !hasThinking {
		return false, "全程无非空 thinking"
	}
	if badTool != "" {
		return false, fmt.Sprintf("tool_use %s 未命中 schema", badTool)
	}

	// tool_result error 率（遍历 full：所有 messages 轮 + 本次 response）。
	nRes, nErr := 0, 0
	countToolResults := func(cont gjson.Result) {
		iterContentBlocks(cont, func(b gjson.Result) {
			if b.Get("type").String() == "tool_result" {
				nRes++
				if b.Get("is_error").Bool() {
					nErr++
				}
			}
		})
	}
	for _, m := range msgs {
		countToolResults(m.Get("content"))
	}
	countToolResults(resp.Get("content"))
	if nRes > 0 && float64(nErr)/float64(nRes) >= packMaxToolErrorRate {
		return false, fmt.Sprintf("tool error 率 %d/%d ≥25%%", nErr, nRes)
	}

	return true, ""
}

// packCallEntry 打包时从单个 call 文件提取的索引信息（用于 manifest / all_calls.jsonl / README）。
type packCallEntry struct {
	fileName    string // call 文件名（不含目录）
	sessionID   string // 顶层 session_id
	requestID   string // 顶层 request_id
	timestamp   string // 顶层 timestamp
	model       string // request.model（用于 README 统计）
	effort      string // 顶层 thinking_effort（用于 README 统计）
	prettyBytes []byte // 原始格式化 JSON（人工抽检入口，写入 sessions/）
	compactJSON []byte // 紧凑 JSON（写入 all_calls.jsonl 一行；并据此算 sha256）
	sha256Hex   string // 紧凑 JSON 的 sha256
}

// packBatchManifest 打包 zip 内 manifest.json 的结构（与交付样本一致）。
type packBatchManifest struct {
	BatchID      string               `json:"batch_id"`
	UpdatedAt    string               `json:"updated_at"`
	Format       string               `json:"format"`
	JSONLRule    string               `json:"jsonl_rule"`
	SessionCount int                  `json:"session_count"`
	CallCount    int                  `json:"call_count"`
	Sessions     []string             `json:"sessions"`
	Records      []packManifestRecord `json:"records"`
}

// packManifestRecord 打包 manifest 内单条 call 记录（含 source_file，与样本对齐）。
type packManifestRecord struct {
	SourceFile        string `json:"source_file"`
	SessionID         string `json:"session_id"`
	RequestID         string `json:"request_id"`
	Timestamp         string `json:"timestamp"`
	CallFile          string `json:"call_file"`
	SHA256CompactJSON string `json:"sha256_compact_json"`
}

// packSegment 把段内 calls 打成 sellable/{sid}__{seqNo:03d}.zip（原子写）。
// zip 内部结构与 v1 一致，batch_id 改为 {sid}__{seqNo:03d} 反映段维度。
func (tc *TrajectoryCollector) packSegment(sid string, seqNo int, entries []packCallEntry) (string, error) {
	if len(entries) == 0 {
		return "", fmt.Errorf("empty segment for session %s seq=%d", sid, seqNo)
	}
	sellableDir := filepath.Join(tc.dir, sellableSubDir)
	if err := os.MkdirAll(sellableDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir sellable: %w", err)
	}

	batchID := fmt.Sprintf("%s__%03d", sanitizePathSegment(sid), seqNo)
	zipPath := filepath.Join(sellableDir, batchID+".zip")
	tmp := zipPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return "", fmt.Errorf("create zip tmp: %w", err)
	}
	zw := zip.NewWriter(f)

	// 失败时统一清理。
	fail := func(format string, a ...any) (string, error) {
		_ = zw.Close()
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", fmt.Errorf(format, a...)
	}

	writeEntry := func(name string, content []byte) error {
		w, cerr := zw.Create(batchID + "/" + name)
		if cerr != nil {
			return cerr
		}
		_, werr := w.Write(content)
		return werr
	}

	// sessions/{sid}/{file}.json —— 人工抽检入口（写格式化原文）。
	// 同时构建 all_calls.jsonl 与 manifest records。
	var jsonlBuf bytes.Buffer
	mf := packBatchManifest{
		BatchID:   batchID,
		UpdatedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
		Format:    "Anthropic native API call-level raw request/response",
		JSONLRule: "one line = one LLM call",
		Sessions:  []string{sid},
	}
	for _, e := range entries {
		relCallPath := "sessions/" + sid + "/" + e.fileName
		if err := writeEntry(relCallPath, e.prettyBytes); err != nil {
			return fail("zip write call file: %w", err)
		}
		jsonlBuf.Write(e.compactJSON)
		jsonlBuf.WriteByte('\n')
		mf.Records = append(mf.Records, packManifestRecord{
			SourceFile:        e.fileName,
			SessionID:         e.sessionID,
			RequestID:         e.requestID,
			Timestamp:         e.timestamp,
			CallFile:          relCallPath,
			SHA256CompactJSON: e.sha256Hex,
		})
	}
	mf.SessionCount = 1
	mf.CallCount = len(entries)

	if err := writeEntry("all_calls.jsonl", jsonlBuf.Bytes()); err != nil {
		return fail("zip write all_calls.jsonl: %w", err)
	}

	mfBytes, merr := json.MarshalIndent(mf, "", "  ")
	if merr != nil {
		return fail("marshal manifest: %w", merr)
	}
	if err := writeEntry("manifest.json", mfBytes); err != nil {
		return fail("zip write manifest.json: %w", err)
	}

	readme := buildPackReadme(batchID, sid, seqNo, entries)
	if err := writeEntry("README.md", []byte(readme)); err != nil {
		return fail("zip write README.md: %w", err)
	}

	if err := zw.Close(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", fmt.Errorf("zip close: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("close zip file: %w", err)
	}
	if err := os.Rename(tmp, zipPath); err != nil {
		return "", fmt.Errorf("rename zip: %w", err)
	}
	return zipPath, nil
}

// buildPackReadme 按当前段实际数据动态生成 README.md（结构与交付样本一致，加上"段"维度说明）。
func buildPackReadme(batchID, sid string, seqNo int, entries []packCallEntry) string {
	model, effort := "", ""
	firstTS, lastTS := "", ""
	if len(entries) > 0 {
		model = entries[0].model
		effort = entries[0].effort
		firstTS = entries[0].timestamp
		lastTS = entries[len(entries)-1].timestamp
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# %s 批次说明\n\n", batchID)
	b.WriteString("本目录为 Anthropic 原生 API call-level 交付批次。\n\n")
	b.WriteString("## 交付粒度\n\n")
	b.WriteString("- 一条 JSONL 记录 = 一次 LLM call 的 raw `request` / `response`。\n")
	b.WriteString("- `request.messages` 是该次 call 发送给模型的完整上下文历史。\n")
	b.WriteString("- 同一真实用户会话下的多次 call 通过相同 `session_id` 关联。\n")
	b.WriteString("- 每次 call 使用独立的 `request_id` 和 `timestamp`。\n")
	b.WriteString("- 同一 session 可分多段交付，本批次为该 session 的第 ")
	fmt.Fprintf(&b, "%d 段。\n\n", seqNo)
	b.WriteString("## 当前批次统计\n\n")
	fmt.Fprintf(&b, "- 批次目录：`%s/`\n", batchID)
	fmt.Fprintf(&b, "- Session ID：`%s`\n", sid)
	fmt.Fprintf(&b, "- 段序号：%03d\n", seqNo)
	b.WriteString("- Session 数：1\n")
	fmt.Fprintf(&b, "- Call 数：%d\n", len(entries))
	if model != "" {
		fmt.Fprintf(&b, "- 模型：`%s`\n", model)
	}
	if effort != "" {
		fmt.Fprintf(&b, "- Thinking effort：`%s`\n", effort)
	}
	fmt.Fprintf(&b, "- 时间范围：`%s` 至 `%s`\n", firstTS, lastTS)
	b.WriteString("- 汇总文件：`all_calls.jsonl`\n")
	b.WriteString("- 清单文件：`manifest.json`\n\n")
	b.WriteString("## 目录结构\n\n")
	b.WriteString("```text\n")
	fmt.Fprintf(&b, "%s/\n", batchID)
	b.WriteString("├── README.md\n")
	b.WriteString("├── manifest.json\n")
	b.WriteString("├── all_calls.jsonl\n")
	b.WriteString("└── sessions/\n")
	fmt.Fprintf(&b, "    └── %s/\n", sid)
	b.WriteString("        └── {timestamp}__{request_id}.json\n")
	b.WriteString("```\n\n")
	b.WriteString("## 本批次记录\n\n")
	b.WriteString("| # | timestamp | request_id |\n")
	b.WriteString("| ---: | --- | --- |\n")
	for i, e := range entries {
		fmt.Fprintf(&b, "| %d | `%s` | `%s` |\n", i+1, e.timestamp, e.requestID)
	}
	b.WriteString("\n## 验收建议\n\n")
	b.WriteString("- 批量脚本验收：优先读取 `all_calls.jsonl`。\n")
	b.WriteString("- 人工抽查：进入 `sessions/{session_id}/` 查看单个 call JSON。\n")
	b.WriteString("- 校验内容一致性时，可按 `manifest.json` 中的 `sha256_compact_json` 复算哈希。\n")
	return b.String()
}

// loadPackStateLocked 读取 pack_state.json。调用方必须持有 packMu。
func (tc *TrajectoryCollector) loadPackStateLocked() *packState {
	st := &packState{Sessions: map[string]packSessionState{}}
	path := filepath.Join(tc.dir, packStateFile)
	if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
		_ = json.Unmarshal(data, st)
		if st.Sessions == nil {
			st.Sessions = map[string]packSessionState{}
		}
	}
	return st
}

// savePackStateLocked 原子写 pack_state.json。调用方必须持有 packMu。
func (tc *TrajectoryCollector) savePackStateLocked(st *packState) {
	path := filepath.Join(tc.dir, packStateFile)
	out, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}

// packStatsSnapshot 汇总打包统计：能卖条数（sellable/ 下 zip 数）、不合格数、待判定数、上次打包时间。
//
// v2 段语义下的字段语义：
//   - sellable：直接数 sellable/*.zip（含 v1 旧 zip 与 v2 分段 zip，跨重启准确）。
//   - unqualified：v2 不再永久判死段，方案 A 都会重试 → 永远返回 0（保留字段做 admin UI 兼容）。
//   - pending：还没打过任何段的 session 数（磁盘上有 sessions/{sid}/ 但 PackedSegments 空）。
func (tc *TrajectoryCollector) packStatsSnapshot() (sellable, unqualified, pending int, lastPackAt string) {
	zips, _ := filepath.Glob(filepath.Join(tc.dir, sellableSubDir, "*.zip"))
	sellable = len(zips)

	tc.packMu.Lock()
	state := tc.loadPackStateLocked()
	tc.packMu.Unlock()

	// 已"开过张"（至少打过一段）的 session 数。
	hasPacked := 0
	for _, s := range state.Sessions {
		if len(s.PackedSegments) > 0 {
			hasPacked++
		}
	}

	// pending = 磁盘 session 总数 - 已打过段的 session 数。
	sessRoot := filepath.Join(tc.dir, "sessions")
	if entries, err := os.ReadDir(sessRoot); err == nil {
		total := 0
		for _, e := range entries {
			if e.IsDir() {
				total++
			}
		}
		pending = total - hasPacked
		if pending < 0 {
			pending = 0
		}
	}

	if v := tc.lastPackAt.Load(); v > 0 {
		lastPackAt = time.Unix(v, 0).UTC().Format("2006-01-02T15:04:05Z")
	}
	return
}
