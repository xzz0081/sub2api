package service

// trajectory_collector.go
// 轨迹采集器：采集器开启时，把【每一次】LLM call 的完整 request / response 按交付格式
//（Anthropic 原生 call-level）落盘，按模型分目录归类。
//
// 设计要点：
//   - 不过滤：不再按 model / effort / thinking.type 判定，也不对请求做任何注入改写，
//     采集器只是旁路记录原始流量，零行为影响。
//   - 按模型分类：落盘路径为 sessions/{model}/{session}/{ts}__{req}.json。
//   - 通过环境变量开关，默认关闭，与现有 SUB2API_DEBUG_* 风格一致，避免改动依赖注入。
//   - 异步写盘，绝不阻塞主转发链路。
//   - 流式响应在网关侧重组为完整 Anthropic message body，thinking/signature/tool_use.input 全保真。

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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

// gin.Context 中存放采集上下文的 key（避免改函数签名）。
const trajectoryCaptureKey = "sub2api_trajectory_capture"

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

// maybeBeginTrajectoryCapture 为本次请求创建采集上下文并挂到 gin.Context。
// 必须在调用 handleStreamingResponse / handleNonStreamingResponse 之前调用。
//
// 不过滤策略：采集器开启时采集【所有】请求，不再按 model / effort / thinking.type 判定，
// 也不对请求做任何注入改写。落盘时按模型分目录归类（见 write）。
func (s *GatewayService) maybeBeginTrajectoryCapture(c *gin.Context, parsed *ParsedRequest, originalModel string, stream bool) {
	if s == nil || !s.trajectoryCollector.Enabled() || c == nil || parsed == nil {
		return
	}
	body := parsed.Body.Bytes()

	// 不过滤：每个请求都建立采集上下文。evaluated/matched 计数保持等值（采集率 100%）。
	s.trajectoryCollector.statEvaluated.Add(1)
	s.trajectoryCollector.statMatched.Add(1)

	reqCopy := make([]byte, len(body))
	copy(reqCopy, body)

	cap := &trajectoryCapture{
		sessionID:      s.GenerateSessionHash(parsed),
		requestID:      "",
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

// TrajectoryCollector 轨迹采集器，负责异步落盘。
type TrajectoryCollector struct {
	dir       string
	enabled   bool
	redact    bool
	startedAt time.Time

	statEvaluated      atomic.Int64
	statMatched        atomic.Int64
	statSubmitted      atomic.Int64
	statSaved          atomic.Int64
	statSaveFailed     atomic.Int64
	statStreamSaved    atomic.Int64
	statNonStreamSaved atomic.Int64
	statInputTokens    atomic.Int64
	statOutputTokens   atomic.Int64
	statModelOpus46    atomic.Int64
	statModelOpus47    atomic.Int64
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

// Submit 异步提交一条采集记录。responseData 为完整 Anthropic message body（已解码）。
//
// 不过滤策略：不再做任何请求侧/响应侧质检（signature / stop_reason / thinking 等一律不卡），
// 只要请求体与响应体非空就原样落盘，按模型分目录归类（见 write）。
func (tc *TrajectoryCollector) Submit(cap *trajectoryCapture, responseData []byte) {
	if !tc.Enabled() || cap == nil {
		return
	}
	if len(responseData) == 0 || len(cap.requestBody) == 0 {
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
	sid := rec.SessionID

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.LegacyPrintf("service.trajectory", "panic while writing trajectory: %v", r)
			}
		}()
		if err := tc.write(rec, model); err != nil {
			tc.statSaveFailed.Add(1)
			logger.LegacyPrintf("service.trajectory", "write trajectory failed: session=%s request=%s err=%v", rec.SessionID, rec.RequestID, err)
			return
		}
		tc.recordSaveStats(model, stream, respCopy)
	}()
}

// recordSaveStats 落盘成功后更新运行时统计。
func (tc *TrajectoryCollector) recordSaveStats(model string, stream bool, responseData []byte) {
	tc.statSaved.Add(1)
	if stream {
		tc.statStreamSaved.Add(1)
	} else {
		tc.statNonStreamSaved.Add(1)
	}
	tc.statInputTokens.Add(gjson.GetBytes(responseData, "usage.input_tokens").Int())
	tc.statOutputTokens.Add(gjson.GetBytes(responseData, "usage.output_tokens").Int())
	lower := strings.ToLower(model)
	switch {
	case strings.Contains(lower, "opus-4-6"):
		tc.statModelOpus46.Add(1)
	case strings.Contains(lower, "opus-4-7"):
		tc.statModelOpus47.Add(1)
	}
}

// marshalNoEscapeIndent 序列化为格式化 JSON，不转义非 ASCII / < > &。
func marshalNoEscapeIndent(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// write 把一条记录写盘：sessions/{model}/{session}/{ts}__{req}.json。
func (tc *TrajectoryCollector) write(rec *trajectoryTopRecord, model string) error {
	pretty, err := marshalNoEscapeIndent(rec)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if tc.redact {
		pretty = tc.redactSecrets(pretty)
	}
	tsFile := strings.NewReplacer(":", "-", ".", "-").Replace(rec.Timestamp)
	sessionDir := filepath.Join(tc.dir, "sessions", sanitizePathSegment(model), sanitizePathSegment(rec.SessionID))
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	callPath := filepath.Join(sessionDir, fmt.Sprintf("%s__%s.json", tsFile, sanitizePathSegment(rec.RequestID)))
	return os.WriteFile(callPath, pretty, 0o644)
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
	Enabled        bool    `json:"enabled"`
	Dir            string  `json:"dir"`
	StartedAt      string  `json:"started_at"`
	UptimeSec      int64   `json:"uptime_sec"`
	Submitted      int64   `json:"submitted"`
	Saved          int64   `json:"saved"`
	SaveFailed     int64   `json:"save_failed"`
	StreamSaved    int64   `json:"stream_saved"`
	NonStreamSaved int64   `json:"non_stream_saved"`
	SaveRate       float64 `json:"save_rate"`
	TotalInputTokens  int64 `json:"total_input_tokens"`
	TotalOutputTokens int64 `json:"total_output_tokens"`
	ModelOpus46    int64   `json:"model_opus_4_6_saved"`
	ModelOpus47    int64   `json:"model_opus_4_7_saved"`
}

// ratio 安全除法，分母为 0 返回 0，结果保留 4 位小数。
func ratio(num, den int64) float64 {
	if den <= 0 {
		return 0
	}
	r := float64(num) / float64(den)
	return float64(int64(r*10000+0.5)) / 10000
}

// Stats 返回当前采集统计快照。
func (tc *TrajectoryCollector) Stats() TrajectoryStats {
	if tc == nil {
		return TrajectoryStats{Enabled: false}
	}
	saved := tc.statSaved.Load()
	return TrajectoryStats{
		Enabled:           tc.enabled,
		Dir:               tc.dir,
		StartedAt:         tc.startedAt.UTC().Format("2006-01-02T15:04:05Z"),
		UptimeSec:         int64(time.Since(tc.startedAt).Seconds()),
		Submitted:         tc.statSubmitted.Load(),
		Saved:             saved,
		SaveFailed:        tc.statSaveFailed.Load(),
		StreamSaved:       tc.statStreamSaved.Load(),
		NonStreamSaved:    tc.statNonStreamSaved.Load(),
		SaveRate:          ratio(saved, tc.statSubmitted.Load()),
		TotalInputTokens:  tc.statInputTokens.Load(),
		TotalOutputTokens: tc.statOutputTokens.Load(),
		ModelOpus46:       tc.statModelOpus46.Load(),
		ModelOpus47:       tc.statModelOpus47.Load(),
	}
}

// readManifestCounts — removed (manifest.json no longer written)

// TrajectoryStats 暴露采集统计快照给上层（handler）。采集器未初始化时返回 disabled。
func (s *GatewayService) TrajectoryStats() TrajectoryStats {
	if s == nil || s.trajectoryCollector == nil {
		return TrajectoryStats{Enabled: false}
	}
	return s.trajectoryCollector.Stats()
}
