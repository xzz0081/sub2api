package service

// trajectory_packer.go
// 轨迹打包器：对采集落盘的 session 做「静默超时 → 质检 → 自动打包」。
//
// 设计要点：
//   - 完全旁路：只读原始落盘数据 + 额外生成 zip，绝不删除/修改任何原始数据。
//   - 后台 goroutine 定时扫描 sessions/，对静默够久（默认 5 分钟无新 call）的 session 跑 validator。
//   - 合格（能卖钱）的 session 压成 sellable/{session_id}.zip；不合格只记原因，不打包。
//   - pack_state.json 记录每个 session 的判定结果，避免重复处理；若 session 之后又有新轮，则重新判定。
//   - 质检标准移植自工作流 SOP 的 validator.py（normal 档）。

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
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/tidwall/gjson"
)

// 打包器相关环境变量名。
const (
	trajectoryPackEnv         = "SUB2API_TRAJECTORY_PACK"              // "1"/"true" 开启自动打包
	trajectoryPackIntervalEnv = "SUB2API_TRAJECTORY_PACK_INTERVAL_SEC" // 扫描间隔（秒），默认 120
	trajectorySilenceEnv      = "SUB2API_TRAJECTORY_SILENCE_SEC"       // 静默判定窗口（秒），默认 300
)

// 打包器默认参数。
const (
	trajectoryPackDefaultInterval = 120 // 默认每 2 分钟扫一次
	trajectoryPackDefaultSilence  = 300 // 默认 session 静默 5 分钟即判定
	sellableSubDir                = "sellable"
	packStateFile                 = "pack_state.json"
	validatorTrack                = "normal"
)

// validator 允许集（normal 档，与 SOP validator.py 对齐）。
var (
	packAllowedModels     = []string{"claude-opus-4-6", "claude-opus-4-7"}
	packAllowedEfforts    = map[string]struct{}{"high": {}, "xhigh": {}, "max": {}}
	packMinAssistantTurns = 5
	packMaxToolErrorRate  = 0.25
)

// packState pack_state.json 的结构：记录每个 session 的判定结果。
type packState struct {
	UpdatedAt string                      `json:"updated_at"`
	Sessions  map[string]packSessionState `json:"sessions"`
}

// packSessionState 单个 session 的判定结果。
type packSessionState struct {
	Status    string `json:"status"`              // qualified（合格已打包）/ unqualified（不合格）
	Reason    string `json:"reason,omitempty"`    // 不合格原因
	CheckedAt string `json:"checked_at"`          // 判定时刻
	ZipFile   string `json:"zip_file,omitempty"`  // 合格时的 zip 文件名
	CallCount int    `json:"call_count"`          // 判定时该 session 的 call 数（用于检测后续新轮）
}

// getEnvIntSeconds 读取一个表示秒数的正整数环境变量，非法或缺失时返回默认值。
func getEnvIntSeconds(env string, def int) int {
	v := strings.TrimSpace(os.Getenv(env))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// runPackLoop 后台定时扫描循环。进程启动后随采集器一起跑，直到进程退出。
func (tc *TrajectoryCollector) runPackLoop() {
	ticker := time.NewTicker(tc.packInterval)
	defer ticker.Stop()
	for range ticker.C {
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.LegacyPrintf("service.trajectory", "panic in pack loop: %v", r)
				}
			}()
			tc.scanAndPack()
		}()
	}
}

// scanAndPack 扫描一遍 sessions/：对静默够久且未处理（或有新轮）的 session 跑质检并打包。
func (tc *TrajectoryCollector) scanAndPack() {
	sessionsRoot := filepath.Join(tc.dir, "sessions")
	entries, err := os.ReadDir(sessionsRoot)
	if err != nil {
		return // sessions 目录还不存在，说明尚无采集数据，直接跳过
	}

	state := tc.loadPackState()
	now := time.Now()
	changed := false

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sid := e.Name()
		sessionDir := filepath.Join(sessionsRoot, sid)

		// 取该 session 最新文件 mtime 与 call 数。
		latest, callCount := tc.latestMtime(sessionDir)
		if callCount == 0 {
			continue
		}

		// 已判定过：仅当之后又有新轮（call 数增加）才重新判定，否则跳过。
		if prev, done := state.Sessions[sid]; done && callCount <= prev.CallCount {
			continue
		}

		// 静默检查：最新文件距今不足静默窗口，说明对话可能还在继续，下次再说。
		if now.Sub(latest) < tc.packSilence {
			continue
		}

		// 跑质检。
		ok, reason := tc.validateSession(sessionDir)
		ss := packSessionState{
			CheckedAt: now.UTC().Format(time.RFC3339),
			CallCount: callCount,
		}
		if ok {
			zipPath, perr := tc.packSession(sid, sessionDir)
			if perr != nil {
				// 打包失败：不写 state，下次重试。
				logger.LegacyPrintf("service.trajectory", "pack session failed: session=%s err=%v", sid, perr)
				continue
			}
			ss.Status = "qualified"
			ss.ZipFile = filepath.Base(zipPath)
			tc.statSellable.Add(1)
			tc.lastPackAt.Store(now.Unix())
			logger.LegacyPrintf("service.trajectory", "session packed (sellable): session=%s calls=%d zip=%s", sid, callCount, ss.ZipFile)
		} else {
			ss.Status = "unqualified"
			ss.Reason = reason
			tc.statUnqualified.Add(1)
			logger.LegacyPrintf("service.trajectory", "session unqualified: session=%s reason=%s", sid, reason)
		}
		state.Sessions[sid] = ss
		changed = true
	}

	if changed {
		state.UpdatedAt = now.UTC().Format(time.RFC3339)
		tc.savePackState(state)
	}
}

// latestMtime 返回 session 目录下所有 *.json 的最新修改时间与文件数。
func (tc *TrajectoryCollector) latestMtime(sessionDir string) (time.Time, int) {
	files, _ := filepath.Glob(filepath.Join(sessionDir, "*.json"))
	var latest time.Time
	for _, fp := range files {
		fi, err := os.Stat(fp)
		if err != nil {
			continue
		}
		if fi.ModTime().After(latest) {
			latest = fi.ModTime()
		}
	}
	return latest, len(files)
}

// loadSessionCalls 读取并解析 session 目录下所有 call 文件（按文件名排序）。
func (tc *TrajectoryCollector) loadSessionCalls(sessionDir string) []gjson.Result {
	files, _ := filepath.Glob(filepath.Join(sessionDir, "*.json"))
	sort.Strings(files)
	out := make([]gjson.Result, 0, len(files))
	for _, fp := range files {
		data, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		if !gjson.ValidBytes(data) {
			continue
		}
		out = append(out, gjson.ParseBytes(data))
	}
	return out
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

// validateSession 对一个 session 跑 normal 档质检，返回（是否合格，不合格原因）。
// 取该 session 里 messages 最长的那个 call 作为「最长 trajectory」进行判定（与 SOP 一致）。
func (tc *TrajectoryCollector) validateSession(sessionDir string) (bool, string) {
	calls := tc.loadSessionCalls(sessionDir)
	if len(calls) == 0 {
		return false, "空目录"
	}

	// 取 messages 最长的 call。
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

	// 末轮完整性（#8 致命项）：stop_reason 必须 end_turn，且有非空 text 总结。
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
	BatchID      string              `json:"batch_id"`
	UpdatedAt    string              `json:"updated_at"`
	Format       string              `json:"format"`
	JSONLRule    string              `json:"jsonl_rule"`
	SessionCount int                 `json:"session_count"`
	CallCount    int                 `json:"call_count"`
	Sessions     []string            `json:"sessions"`
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

// packSession 把一个合格 session 打成 sellable/{session_id}.zip（原子写），
// zip 内部结构与交付样本（deliver_*.zip）完全一致：
//
//	{session_id}/
//	  ├── README.md          批次说明
//	  ├── manifest.json       批次索引 + 每条 call 的 sha256 校验
//	  ├── all_calls.jsonl     程序验收入口，一行一个 call
//	  └── sessions/{sid}/     人工抽检入口，一个 call 一个格式化 JSON 文件
//
// 只读原始 call 文件，不修改/删除任何原始数据。
func (tc *TrajectoryCollector) packSession(sid, sessionDir string) (string, error) {
	sellableDir := filepath.Join(tc.dir, sellableSubDir)
	if err := os.MkdirAll(sellableDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir sellable: %w", err)
	}

	// 1. 收集该 session 下所有 call 文件，逐个解析出索引信息。
	files, _ := filepath.Glob(filepath.Join(sessionDir, "*.json"))
	sort.Strings(files)
	entries := make([]packCallEntry, 0, len(files))
	for _, fp := range files {
		data, rerr := os.ReadFile(fp)
		if rerr != nil {
			continue
		}
		if !gjson.ValidBytes(data) {
			continue // 跳过坏文件，不让单个坏数据毁掉整个 zip
		}
		// 紧凑化：把格式化 JSON 压成单行，供 all_calls.jsonl 与 sha256 使用。
		var compactBuf bytes.Buffer
		if err := json.Compact(&compactBuf, data); err != nil {
			continue
		}
		compact := compactBuf.Bytes()
		sum := sha256.Sum256(compact)
		root := gjson.ParseBytes(data)
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
	if len(entries) == 0 {
		return "", fmt.Errorf("no valid call files in session %s", sid)
	}

	// 2. 准备 zip：所有内容放在 {sid}/ 批次目录下（与样本顶层一致）。
	zipPath := filepath.Join(sellableDir, sanitizePathSegment(sid)+".zip")
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

	batchDir := sanitizePathSegment(sid)
	writeEntry := func(name string, content []byte) error {
		w, cerr := zw.Create(batchDir + "/" + name)
		if cerr != nil {
			return cerr
		}
		_, werr := w.Write(content)
		return werr
	}

	// 3. sessions/{sid}/{file}.json —— 人工抽检入口（写格式化原文）。
	//    同时构建 all_calls.jsonl 与 manifest records。
	var jsonlBuf bytes.Buffer
	mf := packBatchManifest{
		BatchID:   batchDir,
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

	// 4. all_calls.jsonl —— 程序验收入口。
	if err := writeEntry("all_calls.jsonl", jsonlBuf.Bytes()); err != nil {
		return fail("zip write all_calls.jsonl: %w", err)
	}

	// 5. manifest.json —— 批次索引 + 校验。
	mfBytes, merr := json.MarshalIndent(mf, "", "  ")
	if merr != nil {
		return fail("marshal manifest: %w", merr)
	}
	if err := writeEntry("manifest.json", mfBytes); err != nil {
		return fail("zip write manifest.json: %w", err)
	}

	// 6. README.md —— 批次说明（按当前批次实际统计动态生成）。
	readme := buildPackReadme(sid, entries)
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

// buildPackReadme 按当前批次实际数据动态生成 README.md（结构与交付样本一致）。
func buildPackReadme(sid string, entries []packCallEntry) string {
	// 提取模型 / effort / 时间范围（取首条；同 session 下 model 一致）。
	model, effort := "", ""
	firstTS, lastTS := "", ""
	if len(entries) > 0 {
		model = entries[0].model
		effort = entries[0].effort
		firstTS = entries[0].timestamp
		lastTS = entries[len(entries)-1].timestamp
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# %s 批次说明\n\n", sid)
	b.WriteString("本目录为 Anthropic 原生 API call-level 交付批次。\n\n")
	b.WriteString("## 交付粒度\n\n")
	b.WriteString("- 一条 JSONL 记录 = 一次 LLM call 的 raw `request` / `response`。\n")
	b.WriteString("- `request.messages` 是该次 call 发送给模型的完整上下文历史。\n")
	b.WriteString("- 同一真实用户会话下的多次 call 通过相同 `session_id` 关联。\n")
	b.WriteString("- 每次 call 使用独立的 `request_id` 和 `timestamp`。\n\n")
	b.WriteString("## 当前批次统计\n\n")
	fmt.Fprintf(&b, "- 批次目录：`%s/`\n", sid)
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
	fmt.Fprintf(&b, "%s/\n", sid)
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

// loadPackState 读取 pack_state.json（加锁）。
func (tc *TrajectoryCollector) loadPackState() *packState {
	tc.packMu.Lock()
	defer tc.packMu.Unlock()
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

// savePackState 原子写 pack_state.json（加锁）。
func (tc *TrajectoryCollector) savePackState(st *packState) {
	tc.packMu.Lock()
	defer tc.packMu.Unlock()
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
func (tc *TrajectoryCollector) packStatsSnapshot() (sellable, unqualified, pending int, lastPackAt string) {
	// sellable：直接数 sellable/*.zip（跨重启准确，重新打包覆盖同名不重复）。
	zips, _ := filepath.Glob(filepath.Join(tc.dir, sellableSubDir, "*.zip"))
	sellable = len(zips)

	// 从 state 数不合格数。
	state := tc.loadPackState()
	for _, s := range state.Sessions {
		if s.Status == "unqualified" {
			unqualified++
		}
	}

	// pending = sessions 目录总数 - 已判定数。
	sessRoot := filepath.Join(tc.dir, "sessions")
	if entries, err := os.ReadDir(sessRoot); err == nil {
		total := 0
		for _, e := range entries {
			if e.IsDir() {
				total++
			}
		}
		pending = total - len(state.Sessions)
		if pending < 0 {
			pending = 0
		}
	}

	if v := tc.lastPackAt.Load(); v > 0 {
		lastPackAt = time.Unix(v, 0).UTC().Format("2006-01-02T15:04:05Z")
	}
	return
}
