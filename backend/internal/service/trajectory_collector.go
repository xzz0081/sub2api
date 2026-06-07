package service

// trajectory_collector.go
// 轨迹采集器：当请求满足采购条件（opus-4.6/4.7 + thinking adaptive + effort high/xhigh/max）时，
// 把这次 LLM call 的完整 request / response 按交付格式（Anthropic 原生 call-level）落盘。
//
// 设计要点：
//   - 完全旁路：不改转发逻辑、不注入提示词、不动客户端配置。
//   - 通过环境变量开关，默认关闭，与现有 SUB2API_DEBUG_* 风格一致，避免改动依赖注入。
//   - 异步写盘，绝不阻塞主转发链路。
//   - 流式响应在网关侧重组为完整 Anthropic message body，thinking/signature/tool_use.input 全保真。

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

// 采集开关与目录的环境变量名。
const (
	trajectoryCollectEnv = "SUB2API_TRAJECTORY_COLLECT" // "1"/"true" 开启
	trajectoryDirEnv     = "SUB2API_TRAJECTORY_DIR"     // 输出目录，默认 /app/data/trajectories
	trajectoryDefaultDir = "/app/data/trajectories"
)

// 响应侧质检：落盘前最后一道闸，只做结构性判定，不引入硬编码阈值。
// 设计原因：
//   - 上游可能剥离 signature（如 jchpro.com），thinking block 拿不到签名 → 验收红线 REJECT；
//   - 末轮 stop_reason 必须 end_turn，被 tool_use / max_tokens / stop_sequence 截断的不合格。
//
// 注：thinking 文本长度、output_tokens 门槛这类阈值不在这层做——简单问题（"再见"）的合规
// 思考可能就两三个字，硬定阈值会误杀真实样本。打包阶段的 validator 才是抽样质量评估的地方。

// gin.Context 中存放采集上下文的 key（避免改函数签名）。
const trajectoryCaptureKey = "sub2api_trajectory_capture"

// 允许采集的模型前缀（命中其一即视为目标模型）。
var trajectoryAllowedModelMarkers = []string{"opus-4-6", "opus-4-7"}

// 允许采集的 thinking effort 档位。
var trajectoryAllowedEfforts = map[string]struct{}{
	"high":  {},
	"xhigh": {},
	"max":   {},
}

// 脱敏正则：替换疑似 API key / token，避免 PII 残留（不会误伤 thinking 文本或 base64 signature）。
var trajectorySecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`sk-ant-[A-Za-z0-9_\-]{10,}`),
	regexp.MustCompile(`sk-[A-Za-z0-9]{20,}`),
}

// trajectoryCapture 单次请求的采集上下文，挂在 gin.Context 上。
// 同一请求内由单个 goroutine 顺序访问（流式消费 SSE 的循环），无需加锁。
type trajectoryCapture struct {
	sessionID      string
	requestID      string
	model          string    // 原始客户端模型名（用于落盘）
	thinkingEffort string    // high / xhigh / max
	timestamp      time.Time // 请求开始时刻（UTC）
	requestBody    []byte    // 原始 Anthropic request body（副本）
	streamEvents   []string  // 流式：累积每个 SSE event 的 data JSON（已是发给客户端的最终形态）
	stream         bool
}

// appendStreamEvent 累积一个流式 SSE data 事件（去掉 "data: " 前缀后的 JSON 字符串）。
func (cap *trajectoryCapture) appendStreamEvent(data string) {
	if cap == nil {
		return
	}
	if data == "" || data == "[DONE]" {
		return
	}
	cap.streamEvents = append(cap.streamEvents, data)
}

// maybeBeginTrajectoryCapture 在请求满足采购条件时创建采集上下文并挂到 gin.Context。
// 必须在调用 handleStreamingResponse / handleNonStreamingResponse 之前调用。
// 完全旁路：判断失败或采集器关闭时静默返回，不影响转发。
func (s *GatewayService) maybeBeginTrajectoryCapture(c *gin.Context, parsed *ParsedRequest, originalModel string, stream bool) {
	if s == nil || !s.trajectoryCollector.Enabled() || c == nil || parsed == nil {
		return
	}
	body := parsed.Body.Bytes()
	// 进入判定即计数 evaluated。
	s.trajectoryCollector.statEvaluated.Add(1)
	if !shouldCollectTrajectory(originalModel, parsed.OutputEffort, body) {
		return
	}
	// 命中采集条件。
	s.trajectoryCollector.statMatched.Add(1)
	// 复制原始客户端请求体，避免后续 wire body 改写影响采集内容。
	reqCopy := make([]byte, len(body))
	copy(reqCopy, body)

	cap := &trajectoryCapture{
		sessionID:      s.GenerateSessionHash(parsed),
		requestID:      "", // 上游 x-request-id 在 response 阶段补齐，缺失时生成本地 ID
		model:          originalModel,
		thinkingEffort: strings.ToLower(strings.TrimSpace(parsed.OutputEffort)),
		timestamp:      time.Now().UTC(),
		requestBody:    reqCopy,
		stream:         stream,
	}
	if cap.sessionID == "" {
		cap.sessionID = uuid.NewString()
	}
	c.Set(trajectoryCaptureKey, cap)
}

// finalizeRequestID 确保 capture 有 request_id：优先用上游 x-request-id，缺失则生成本地 ID。
func (cap *trajectoryCapture) finalizeRequestID(upstreamRequestID string) {
	if cap == nil {
		return
	}
	if cap.requestID != "" {
		return
	}
	if strings.TrimSpace(upstreamRequestID) != "" {
		cap.requestID = strings.TrimSpace(upstreamRequestID)
		return
	}
	cap.requestID = newLocalRequestID()
}

// getTrajectoryCapture 从 gin.Context 取采集上下文，未设置返回 nil。
func getTrajectoryCapture(c interface{ Get(string) (any, bool) }) *trajectoryCapture {
	if c == nil {
		return nil
	}
	v, ok := c.Get(trajectoryCaptureKey)
	if !ok {
		return nil
	}
	cap, _ := v.(*trajectoryCapture)
	return cap
}

// shouldCollectTrajectory 判断本次请求是否满足采购条件。
// model：原始客户端模型名；effort：thinking_effort（来自 output_config.effort）；body：原始请求体。
func shouldCollectTrajectory(model, effort string, body []byte) bool {
	// 1. 模型必须是 opus-4.6 / 4.7
	lower := strings.ToLower(model)
	matchedModel := false
	for _, marker := range trajectoryAllowedModelMarkers {
		if strings.Contains(lower, marker) {
			matchedModel = true
			break
		}
	}
	if !matchedModel {
		return false
	}
	// 2. effort 必须 ∈ {high, xhigh, max}
	if _, ok := trajectoryAllowedEfforts[strings.ToLower(strings.TrimSpace(effort))]; !ok {
		return false
	}
	// 3. thinking.type 必须为 adaptive
	if gjson.GetBytes(body, "thinking.type").String() != "adaptive" {
		return false
	}
	return true
}

// TrajectoryCollector 轨迹采集器，负责异步落盘与 manifest 维护。
type TrajectoryCollector struct {
	dir        string
	enabled    bool
	mu         sync.Mutex // 保护 all_calls.jsonl 追加与 manifest 读改写
	redact     bool
	warnedOnce sync.Once
	startedAt  time.Time // 进程启动（采集器创建）时刻，用于 stats

	// 运行时统计计数器（atomic，本进程累计；跨重启清零，磁盘累计另从 manifest 读）。
	statEvaluated     atomic.Int64 // 进入采集判定的请求数
	statMatched       atomic.Int64 // 命中采集条件（opus+thinking+effort）的请求数
	statSubmitted     atomic.Int64 // 调用 Submit 的次数（命中且响应非空）
	statSaved         atomic.Int64 // 实际落盘成功数
	statSaveFailed    atomic.Int64 // 落盘失败数
	statStreamSaved   atomic.Int64 // 流式落盘成功数
	statNonStreamSaved atomic.Int64 // 非流式落盘成功数
	statEndTurnSaved  atomic.Int64 // 末轮 stop_reason==end_turn 的落盘数（合格末轮）
	statToolUseEndSaved atomic.Int64 // 末轮 stop_reason==tool_use 的落盘数（不合格末轮）
	statInputTokens   atomic.Int64 // 累计 input_tokens
	statOutputTokens  atomic.Int64 // 累计 output_tokens
	statModelOpus46   atomic.Int64 // opus-4-6 落盘数
	statModelOpus47   atomic.Int64 // opus-4-7 落盘数

	// 响应侧质检拒收计数（落盘前过滤掉的废数据，只看结构，不设硬编码阈值）。
	statRejectedNoSignature   atomic.Int64 // thinking 块缺 signature 字段（上游剥离）
	statRejectedBadStopReason atomic.Int64 // stop_reason 不是 end_turn（被工具调用或长度截断）

	// 自动打包器（packer）相关：静默扫描 → 质检 → 合格自动 zip。
	packEnabled  bool          // 是否开启自动打包
	packInterval time.Duration // 后台扫描间隔
	packSilence  time.Duration // session 静默判定窗口
	packMu       sync.Mutex    // 保护 pack_state.json 读改写

	statSellable    atomic.Int64 // 累计打包成功（能卖钱）的 session 数
	statUnqualified atomic.Int64 // 累计判定不合格的 session 数
	lastPackAt      atomic.Int64 // 上次成功打包的 unix 时刻（秒）
}

// newTrajectoryCollectorFromEnv 按环境变量构造采集器；未开启时返回 enabled=false 的实例。
func newTrajectoryCollectorFromEnv() *TrajectoryCollector {
	enabled := parseDebugEnvBool(os.Getenv(trajectoryCollectEnv))
	dir := strings.TrimSpace(os.Getenv(trajectoryDirEnv))
	if dir == "" {
		dir = trajectoryDefaultDir
	}
	tc := &TrajectoryCollector{
		dir:       dir,
		enabled:   enabled,
		redact:    true,
		startedAt: time.Now().UTC(),
	}
	if enabled {
		logger.LegacyPrintf("service.trajectory", "Trajectory collector enabled, output dir=%s", dir)
	}

	// 自动打包器：仅在采集开启且 pack 开关打开时启动后台扫描 goroutine。
	tc.packEnabled = enabled && parseDebugEnvBool(os.Getenv(trajectoryPackEnv))
	tc.packInterval = time.Duration(getEnvIntSeconds(trajectoryPackIntervalEnv, trajectoryPackDefaultInterval)) * time.Second
	tc.packSilence = time.Duration(getEnvIntSeconds(trajectorySilenceEnv, trajectoryPackDefaultSilence)) * time.Second
	if tc.packEnabled {
		logger.LegacyPrintf("service.trajectory",
			"Trajectory packer enabled, scan_interval=%s silence_window=%s sellable_dir=%s",
			tc.packInterval, tc.packSilence, filepath.Join(dir, sellableSubDir))
		go tc.runPackLoop()
	}
	return tc
}

// Enabled 返回采集器是否开启。
func (tc *TrajectoryCollector) Enabled() bool {
	return tc != nil && tc.enabled
}

// trajectoryTopRecord 交付格式的顶层结构。
type trajectoryTopRecord struct {
	SessionID      string                 `json:"session_id"`
	RequestID      string                 `json:"request_id"`
	Timestamp      string                 `json:"timestamp"`
	ThinkingEffort string                 `json:"thinking_effort"`
	Request        json.RawMessage        `json:"request"`
	Response       trajectoryResponseWrap `json:"response"`
}

type trajectoryResponseWrap struct {
	ResponseData json.RawMessage `json:"response_data"`
}

// qualityCheckResponse 在 Submit 入口对响应做「落盘级」结构性质检（对齐需求 §3.3 落盘硬规则）。
// 返回空串表示通过。reject 时调用方负责递增对应计数器，本函数不动 stat。
//
// 关键区分（之前在此处犯过的错）：
//   - 本函数是「落盘层」闸门，只按 §3.3 判定单条 call 是否满足"原样落盘"的硬条件。
//   - 「末轮必须 end_turn 且含 summary」是 §4 的「打包卖钱质量门槛」，由 packer 的
//     validateSession 评估整个 session（取最长 trajectory）时判定，绝不在落盘层逐条拒收。
//   - 因此 stop_reason==tool_use 的中间轮是合法且【必须保存】的（铁律 §1.1：每一次 LLM call
//     都要保存，不能丢失中间轮次）。end_turn 仅影响打包评估与统计计数，不影响是否落盘。
//
// 落盘质检规则（任意一条不满足即拒收，对齐 §3.3）：
//  1. stop_reason 非空且 ∈ {end_turn, tool_use, max_tokens, stop_sequence}。
//  2. 「每个」thinking 块都必须 thinking 非空且 signature 非空（§3.3 是条件句：有 thinking 块才
//     要求签名，缺/空 signature 视为上游剥签名的不合规响应，红线）。
//
// 关键区分（之前在此处犯过的第二个错）：
//   - §3.3 落盘层只约束「有 thinking 块的轮」——没有 thinking 块的轮（如纯 text 的 end_turn
//     总结末轮）是【合法的】，必须照常落盘，绝不能因「这一轮没思考块」就拒收。
//   - 「整个 session 至少要有 1 轮非空 thinking」是 §4.5 的打包门槛，由 packer 评估整条
//     trajectory 时判定，绝不在落盘层逐条拒收。
//   - 旧逻辑 `!hasThinkingBlock` 把无思考块的合法轮（尤其 end_turn 总结末轮）错杀，导致
//     session 永远凑不齐合格末轮、永远进不了 sellable。
func qualityCheckResponse(responseData []byte) string {
	// 规则 1：stop_reason 必须非空且在允许集合内（tool_use 等中间轮合法，必须放行落盘）。
	switch gjson.GetBytes(responseData, "stop_reason").String() {
	case "end_turn", "tool_use", "max_tokens", "stop_sequence":
		// 合法 stop_reason，放行
	default:
		// 仅 null 或集合外的非法值才拒收（§3.3：stop_reason 不能为 null）。
		return "bad_stop_reason"
	}
	// 规则 2：遍历 content，「每个」thinking 块都必须 thinking 非空且 signature 非空。
	// 没有 thinking 块 → 合法放行（§3.3 是条件句，不是「必须有 thinking 块」）。
	var badThinking bool
	gjson.GetBytes(responseData, "content").ForEach(func(_, item gjson.Result) bool {
		if item.Get("type").String() != "thinking" {
			return true // 非 thinking 块，跳过
		}
		think := strings.TrimSpace(item.Get("thinking").String())
		sig := strings.TrimSpace(item.Get("signature").String())
		if think == "" || sig == "" {
			badThinking = true // 任一 thinking 块缺 thinking 或 signature 即不合规
			return false       // 提前结束遍历
		}
		return true
	})
	if badThinking {
		return "no_signature"
	}
	return ""
}

// Submit 异步提交一条采集记录。responseData 为完整 Anthropic message body（已解码）。
func (tc *TrajectoryCollector) Submit(cap *trajectoryCapture, responseData []byte) {
	if !tc.Enabled() || cap == nil {
		return
	}
	if len(responseData) == 0 || len(cap.requestBody) == 0 {
		return
	}
	// 响应侧质检：落盘前最后一道闸，过不去的样本计入 rejected_*，绝不写盘。
	if reason := qualityCheckResponse(responseData); reason != "" {
		switch reason {
		case "no_signature":
			tc.statRejectedNoSignature.Add(1)
		case "bad_stop_reason":
			tc.statRejectedBadStopReason.Add(1)
		}
		logger.LegacyPrintf("service.trajectory", "reject sample: session=%s reason=%s", cap.sessionID, reason)
		return
	}
	// 复制一份，避免与主链路共享底层数组。
	reqCopy := make([]byte, len(cap.requestBody))
	copy(reqCopy, cap.requestBody)
	respCopy := make([]byte, len(responseData))
	copy(respCopy, responseData)

	rec := &trajectoryTopRecord{
		SessionID:      cap.sessionID,
		RequestID:      cap.requestID,
		Timestamp:      cap.timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
		ThinkingEffort: strings.ToLower(strings.TrimSpace(cap.thinkingEffort)),
		Request:        json.RawMessage(reqCopy),
		Response:       trajectoryResponseWrap{ResponseData: json.RawMessage(respCopy)},
	}

	tc.statSubmitted.Add(1)
	model := cap.model
	stream := cap.stream

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.LegacyPrintf("service.trajectory", "panic while writing trajectory: %v", r)
			}
		}()
		if err := tc.write(rec); err != nil {
			tc.statSaveFailed.Add(1)
			logger.LegacyPrintf("service.trajectory", "write trajectory failed: session=%s request=%s err=%v", rec.SessionID, rec.RequestID, err)
			return
		}
		// 落盘成功，更新统计。
		tc.recordSaveStats(model, stream, respCopy)
	}()
}

// recordSaveStats 在一条记录成功落盘后更新运行时统计：末轮合格性、token、模型分布。
func (tc *TrajectoryCollector) recordSaveStats(model string, stream bool, responseData []byte) {
	tc.statSaved.Add(1)
	if stream {
		tc.statStreamSaved.Add(1)
	} else {
		tc.statNonStreamSaved.Add(1)
	}

	// 末轮合格性：stop_reason==end_turn 为合格末轮，tool_use 为不合格末轮。
	switch gjson.GetBytes(responseData, "stop_reason").String() {
	case "end_turn":
		tc.statEndTurnSaved.Add(1)
	case "tool_use":
		tc.statToolUseEndSaved.Add(1)
	}

	// token 累计（取本次 call response.usage）。
	tc.statInputTokens.Add(gjson.GetBytes(responseData, "usage.input_tokens").Int())
	tc.statOutputTokens.Add(gjson.GetBytes(responseData, "usage.output_tokens").Int())

	// 模型分布。
	lower := strings.ToLower(model)
	switch {
	case strings.Contains(lower, "opus-4-6"):
		tc.statModelOpus46.Add(1)
	case strings.Contains(lower, "opus-4-7"):
		tc.statModelOpus47.Add(1)
	}
}

// marshalNoEscapeIndent 等价于 json.MarshalIndent，但不把中文等非 ASCII 转成 \uXXXX、
// 也不转义 < > &，使落盘文件中文直接可读，与交付样本（deliver_*）保持一致。
func marshalNoEscapeIndent(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encoder.Encode 会在末尾追加一个换行，去掉以与 MarshalIndent 行为一致。
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// marshalNoEscape 等价于 json.Marshal，但不转义非 ASCII / < > &（用于 jsonl 紧凑行）。
func marshalNoEscape(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// write 把一条记录写盘：sessions/{session}/{ts}__{req}.json + 追加 all_calls.jsonl + 更新 manifest.json。
func (tc *TrajectoryCollector) write(rec *trajectoryTopRecord) error {
	// 序列化（格式化版本用于人工抽检）。中文不转义，保持与交付样本一致。
	pretty, err := marshalNoEscapeIndent(rec)
	if err != nil {
		return fmt.Errorf("marshal pretty: %w", err)
	}
	// 紧凑版本用于 jsonl 与 sha256 校验。
	compact, err := marshalNoEscape(rec)
	if err != nil {
		return fmt.Errorf("marshal compact: %w", err)
	}
	if tc.redact {
		pretty = tc.redactSecrets(pretty)
		compact = tc.redactSecrets(compact)
	}

	// 文件名时间戳：冒号/点替换为短横，符合交付样本命名。
	tsFile := strings.NewReplacer(":", "-", ".", "-").Replace(rec.Timestamp)
	sessionDir := filepath.Join(tc.dir, "sessions", sanitizePathSegment(rec.SessionID))
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		return fmt.Errorf("mkdir session dir: %w", err)
	}
	callFileName := fmt.Sprintf("%s__%s.json", tsFile, sanitizePathSegment(rec.RequestID))
	callPath := filepath.Join(sessionDir, callFileName)
	if err := os.WriteFile(callPath, pretty, 0o644); err != nil {
		return fmt.Errorf("write call file: %w", err)
	}

	// 串行化追加 jsonl 与更新 manifest。
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.appendJSONL(compact); err != nil {
		return fmt.Errorf("append jsonl: %w", err)
	}

	relCallPath := filepath.ToSlash(filepath.Join("sessions", sanitizePathSegment(rec.SessionID), callFileName))
	sum := sha256.Sum256(compact)
	if err := tc.updateManifest(rec, relCallPath, hex.EncodeToString(sum[:])); err != nil {
		return fmt.Errorf("update manifest: %w", err)
	}
	return nil
}

// appendJSONL 向 all_calls.jsonl 追加一行紧凑 JSON。
func (tc *TrajectoryCollector) appendJSONL(compact []byte) error {
	path := filepath.Join(tc.dir, "all_calls.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	line := append(append([]byte{}, compact...), '\n')
	_, err = f.Write(line)
	return err
}

// manifestFile manifest.json 的结构。
type manifestFile struct {
	BatchID      string             `json:"batch_id"`
	UpdatedAt    string             `json:"updated_at"`
	Format       string             `json:"format"`
	JSONLRule    string             `json:"jsonl_rule"`
	SessionCount int                `json:"session_count"`
	CallCount    int                `json:"call_count"`
	Sessions     []string           `json:"sessions"`
	Records      []manifestRecord   `json:"records"`
	sessionSet   map[string]struct{} `json:"-"`
}

type manifestRecord struct {
	SessionID        string `json:"session_id"`
	RequestID        string `json:"request_id"`
	Timestamp        string `json:"timestamp"`
	CallFile         string `json:"call_file"`
	SHA256CompactJSON string `json:"sha256_compact_json"`
}

// updateManifest 读-改-写 manifest.json，追加本条记录并刷新统计。
func (tc *TrajectoryCollector) updateManifest(rec *trajectoryTopRecord, relCallPath, sha string) error {
	path := filepath.Join(tc.dir, "manifest.json")
	mf := &manifestFile{
		BatchID:   "trajectories",
		Format:    "Anthropic native API call-level raw request/response",
		JSONLRule: "one line = one LLM call",
	}
	if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
		// 已存在则载入，忽略解析失败（用新结构覆盖）。
		_ = json.Unmarshal(data, mf)
	}
	mf.sessionSet = make(map[string]struct{}, len(mf.Sessions))
	for _, s := range mf.Sessions {
		mf.sessionSet[s] = struct{}{}
	}

	// 去重：同 request_id 已存在则不重复追加。
	for _, r := range mf.Records {
		if r.RequestID == rec.RequestID {
			return nil
		}
	}

	mf.Records = append(mf.Records, manifestRecord{
		SessionID:        rec.SessionID,
		RequestID:        rec.RequestID,
		Timestamp:        rec.Timestamp,
		CallFile:         relCallPath,
		SHA256CompactJSON: sha,
	})
	if _, ok := mf.sessionSet[rec.SessionID]; !ok {
		mf.sessionSet[rec.SessionID] = struct{}{}
		mf.Sessions = append(mf.Sessions, rec.SessionID)
	}
	mf.SessionCount = len(mf.Sessions)
	mf.CallCount = len(mf.Records)
	mf.UpdatedAt = time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")

	out, err := marshalNoEscapeIndent(mf)
	if err != nil {
		return err
	}
	// 原子写：先写临时文件再 rename。
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// redactSecrets 替换疑似密钥的子串为占位符。
func (tc *TrajectoryCollector) redactSecrets(data []byte) []byte {
	for _, re := range trajectorySecretPatterns {
		data = re.ReplaceAll(data, []byte("[REDACTED_SECRET]"))
	}
	return data
}

// sanitizePathSegment 清洗路径片段，去掉分隔符等危险字符，防止目录穿越。
func sanitizePathSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", "..", "_", ":", "_", "\x00", "")
	return replacer.Replace(s)
}

// =============================================================================
// SSE → Anthropic message 重组
// =============================================================================

// reassembleAnthropicMessage 把流式 SSE 的 data 事件序列重组为完整的 Anthropic message body。
// 输入是按到达顺序排列的每个 event 的 data JSON 字符串。
// 输出是 message_start 的 message 骨架，填充 content blocks、stop_reason、usage 后的完整对象。
func reassembleAnthropicMessage(events []string) ([]byte, error) {
	var message map[string]any
	// partialJSON 记录每个 tool_use block 的 input_json_delta 累积串。
	partialJSON := make(map[int]*strings.Builder)

	for _, raw := range events {
		var ev map[string]any
		if err := json.Unmarshal([]byte(raw), &ev); err != nil {
			continue // 非 JSON（如残留控制行）直接跳过
		}
		evType, _ := ev["type"].(string)
		switch evType {
		case "message_start":
			if msg, ok := ev["message"].(map[string]any); ok {
				message = msg
				if _, ok := message["content"]; !ok {
					message["content"] = []any{}
				}
			}

		case "content_block_start":
			if message == nil {
				continue
			}
			idx := toInt(ev["index"])
			block, _ := ev["content_block"].(map[string]any)
			if block == nil {
				block = map[string]any{}
			}
			ensureContentLen(message, idx+1)
			content := message["content"].([]any)
			content[idx] = block
			message["content"] = content
			// tool_use 的 input 通过 input_json_delta 累积，先准备 builder。
			if bt, _ := block["type"].(string); bt == "tool_use" {
				partialJSON[idx] = &strings.Builder{}
			}

		case "content_block_delta":
			if message == nil {
				continue
			}
			idx := toInt(ev["index"])
			delta, _ := ev["delta"].(map[string]any)
			if delta == nil {
				continue
			}
			content, ok := message["content"].([]any)
			if !ok || idx < 0 || idx >= len(content) {
				continue
			}
			block, _ := content[idx].(map[string]any)
			if block == nil {
				continue
			}
			switch dt, _ := delta["type"].(string); dt {
			case "text_delta":
				block["text"] = asString(block["text"]) + asString(delta["text"])
			case "thinking_delta":
				block["thinking"] = asString(block["thinking"]) + asString(delta["thinking"])
			case "signature_delta":
				block["signature"] = asString(block["signature"]) + asString(delta["signature"])
			case "input_json_delta":
				if b := partialJSON[idx]; b != nil {
					b.WriteString(asString(delta["partial_json"]))
				}
			}

		case "content_block_stop":
			if message == nil {
				continue
			}
			idx := toInt(ev["index"])
			content, ok := message["content"].([]any)
			if !ok || idx < 0 || idx >= len(content) {
				continue
			}
			block, _ := content[idx].(map[string]any)
			if block == nil {
				continue
			}
			// tool_use：把累积的 partial_json 解析为对象写入 input。
			if b := partialJSON[idx]; b != nil {
				jsonStr := strings.TrimSpace(b.String())
				if jsonStr == "" {
					block["input"] = map[string]any{}
				} else {
					var input any
					if err := json.Unmarshal([]byte(jsonStr), &input); err == nil {
						block["input"] = input
					} else {
						block["input"] = map[string]any{}
					}
				}
			}

		case "message_delta":
			if message == nil {
				continue
			}
			if delta, ok := ev["delta"].(map[string]any); ok {
				if sr, exists := delta["stop_reason"]; exists {
					message["stop_reason"] = sr
				}
				if ss, exists := delta["stop_sequence"]; exists {
					message["stop_sequence"] = ss
				}
			}
			// usage：message_delta 携带最终 output_tokens 等，合并进 message.usage。
			if u, ok := ev["usage"].(map[string]any); ok {
				mergeUsageInto(message, u)
			}

		case "message_stop":
			// 流结束，无额外字段。
		}
	}

	if message == nil {
		return nil, fmt.Errorf("no message_start event in stream")
	}
	return json.Marshal(message)
}

// ensureContentLen 确保 message.content 至少有 n 个元素，不足补 nil。
func ensureContentLen(message map[string]any, n int) {
	content, ok := message["content"].([]any)
	if !ok {
		content = []any{}
	}
	for len(content) < n {
		content = append(content, map[string]any{})
	}
	message["content"] = content
}

// mergeUsageInto 把 delta usage 合并到 message.usage（覆盖同名字段）。
func mergeUsageInto(message map[string]any, delta map[string]any) {
	usage, ok := message["usage"].(map[string]any)
	if !ok {
		usage = map[string]any{}
	}
	for k, v := range delta {
		usage[k] = v
	}
	message["usage"] = usage
}

// toInt 把 any（通常 float64）转为 int。
func toInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
	}
	return 0
}

// 生成本地 request_id（上游未返回 x-request-id 时使用），与交付样本风格一致。
func newLocalRequestID() string {
	return "req_local_" + uuid.NewString()
}

// TrajectoryStats 采集统计快照，供 admin 接口返回 JSON。
type TrajectoryStats struct {
	Enabled   bool   `json:"enabled"`    // 采集器是否开启
	Dir       string `json:"dir"`        // 落盘目录
	StartedAt string `json:"started_at"` // 采集器创建（进程启动）时刻
	UptimeSec int64  `json:"uptime_sec"` // 运行时长（秒）

	// 运行时计数（本进程累计，重启清零）。
	Evaluated      int64 `json:"evaluated"`        // 进入采集判定的请求数
	Matched        int64 `json:"matched"`          // 命中采集条件的请求数
	Submitted      int64 `json:"submitted"`        // 提交落盘的次数
	Saved          int64 `json:"saved"`            // 落盘成功数
	SaveFailed     int64 `json:"save_failed"`      // 落盘失败数
	StreamSaved    int64 `json:"stream_saved"`     // 流式落盘成功数
	NonStreamSaved int64 `json:"non_stream_saved"` // 非流式落盘成功数

	// 末轮合格性（验收关键：末轮必须 end_turn 才合格）。
	EndTurnSaved  int64 `json:"end_turn_saved"`  // 末轮 end_turn 数（合格末轮）
	ToolUseEnd    int64 `json:"tool_use_end_saved"` // 末轮 tool_use 数（不合格末轮，需补完整结束轮）

	// 响应侧质检拒收（落盘前过滤掉的废数据，按原因分桶）。仅保留无阈值的结构性规则。
	RejectedNoSignature   int64   `json:"rejected_no_signature"`    // thinking 块缺 signature（上游剥离）
	RejectedBadStopReason int64   `json:"rejected_bad_stop_reason"` // stop_reason 不是 end_turn
	RejectedTotal         int64   `json:"rejected_total"`           // 各项之和，便于一眼看废数据总量
	RejectRate            float64 `json:"reject_rate"`              // rejected_total / matched（多少命中样本被废）

	// token 累计。
	TotalInputTokens  int64 `json:"total_input_tokens"`
	TotalOutputTokens int64 `json:"total_output_tokens"`

	// 模型分布。
	ModelOpus46 int64 `json:"model_opus_4_6_saved"`
	ModelOpus47 int64 `json:"model_opus_4_7_saved"`

	// 比率（0~1，保留 4 位小数）。
	MatchRate   float64 `json:"match_rate"`     // matched / evaluated
	SaveRate    float64 `json:"save_rate"`      // saved / submitted
	EndTurnRate float64 `json:"end_turn_rate"`  // end_turn_saved / saved（末轮合格率）

	// 磁盘累计（从 manifest.json 读，跨重启准确）。
	DiskSessionCount int `json:"disk_session_count"`
	DiskCallCount    int `json:"disk_call_count"`

	// 自动打包（能卖钱的合格数据）相关。
	PackEnabled      bool   `json:"pack_enabled"`       // 是否开启自动打包
	SellableDir      string `json:"sellable_dir"`       // 合格 zip 存放目录
	SellableCount    int    `json:"sellable_count"`     // 已打包（能卖钱）的 session 数 = sellable/ 下 zip 数
	UnqualifiedCount int    `json:"unqualified_count"`  // 判定不合格的 session 数
	PendingCount     int    `json:"pending_count"`      // 尚未判定（可能还在对话或静默期内）的 session 数
	LastPackAt       string `json:"last_pack_at"`       // 上次成功打包时刻（UTC），无则空串
}

// ratio 安全除法，分母为 0 返回 0，结果保留 4 位小数。
func ratio(num, den int64) float64 {
	if den <= 0 {
		return 0
	}
	r := float64(num) / float64(den)
	// 保留 4 位小数。
	return float64(int64(r*10000+0.5)) / 10000
}

// Stats 返回当前采集统计快照（运行时计数 + 磁盘累计）。
func (tc *TrajectoryCollector) Stats() TrajectoryStats {
	if tc == nil {
		return TrajectoryStats{Enabled: false}
	}
	saved := tc.statSaved.Load()
	st := TrajectoryStats{
		Enabled:           tc.enabled,
		Dir:               tc.dir,
		StartedAt:         tc.startedAt.UTC().Format("2006-01-02T15:04:05Z"),
		UptimeSec:         int64(time.Since(tc.startedAt).Seconds()),
		Evaluated:         tc.statEvaluated.Load(),
		Matched:           tc.statMatched.Load(),
		Submitted:         tc.statSubmitted.Load(),
		Saved:             saved,
		SaveFailed:        tc.statSaveFailed.Load(),
		StreamSaved:       tc.statStreamSaved.Load(),
		NonStreamSaved:    tc.statNonStreamSaved.Load(),
		EndTurnSaved:      tc.statEndTurnSaved.Load(),
		ToolUseEnd:        tc.statToolUseEndSaved.Load(),
		RejectedNoSignature:   tc.statRejectedNoSignature.Load(),
		RejectedBadStopReason: tc.statRejectedBadStopReason.Load(),
		TotalInputTokens:  tc.statInputTokens.Load(),
		TotalOutputTokens: tc.statOutputTokens.Load(),
		ModelOpus46:       tc.statModelOpus46.Load(),
		ModelOpus47:       tc.statModelOpus47.Load(),
	}
	st.RejectedTotal = st.RejectedNoSignature + st.RejectedBadStopReason
	st.MatchRate = ratio(st.Matched, st.Evaluated)
	st.SaveRate = ratio(st.Saved, st.Submitted)
	st.EndTurnRate = ratio(st.EndTurnSaved, saved)
	st.RejectRate = ratio(st.RejectedTotal, st.Matched)

	// 磁盘累计：读 manifest.json（失败则保持 0，不影响其余字段）。
	sessions, calls := tc.readManifestCounts()
	st.DiskSessionCount = sessions
	st.DiskCallCount = calls

	// 自动打包统计（能卖钱的合格数据）。
	st.PackEnabled = tc.packEnabled
	st.SellableDir = filepath.Join(tc.dir, sellableSubDir)
	sellable, unqualified, pending, lastPackAt := tc.packStatsSnapshot()
	st.SellableCount = sellable
	st.UnqualifiedCount = unqualified
	st.PendingCount = pending
	st.LastPackAt = lastPackAt
	return st
}

// readManifestCounts 从 manifest.json 读取累计 session / call 数量，读不到返回 0,0。
func (tc *TrajectoryCollector) readManifestCounts() (int, int) {
	path := filepath.Join(tc.dir, "manifest.json")
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return 0, 0
	}
	var mf manifestFile
	if err := json.Unmarshal(data, &mf); err != nil {
		return 0, 0
	}
	return mf.SessionCount, mf.CallCount
}

// TrajectoryStats 暴露采集统计快照给上层（handler）。采集器未初始化时返回 disabled。
func (s *GatewayService) TrajectoryStats() TrajectoryStats {
	if s == nil || s.trajectoryCollector == nil {
		return TrajectoryStats{Enabled: false}
	}
	return s.trajectoryCollector.Stats()
}
