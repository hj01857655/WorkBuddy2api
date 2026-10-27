// Package workbuddy 对应 Python 版 lanes/workbuddy.py：WorkBuddy 上游客户端
// ——请求头、模型目录、聊天 SSE 解析与聚合。
//
// 上游有三个实测约束，直接决定了这里的实现形态（见《聊天接口规格》）：
//
//	· **只支持流式**——body 里 stream 必须是 true，否则返回 400/code 11101；
//	  客户端的非流式请求由网关在服务端聚合 SSE 后再补齐成一份 chat.completion。
//	· **进行中的 finish_reason 是空串**（不是 null），且 usage 内联在收尾帧、
//	  那一帧 choices 非空——所以既不能按 null 判结束，也找不到「空 choices 的 usage 帧」。
//	· **tool_choice 只接受字符串**（auto/none/required），传对象必 400。
//
// 请求头分工同样照抄官方：会话级头（X-Conversation-*）整场不变，请求级头逐请求生成。
// 会话级头若逐请求变化，一次桌面操作会在网关侧被拆成几十个会话。
package workbuddy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"workbuddy2api/internal/cred"
)

const (
	AppVersion   = "5.6.2"
	MaxBodyBytes = 33554432 // 上游请求体上限 32MB
	// 官方总超时 570s；另加 connect 超时 10s（Python 版 timeout=(10,570) 语义）。
	upstreamTimeout = 570 * time.Second
	configPath      = "/v3/config"
	chatPath        = "/v2/chat/completions"

	// /v3/config 的模型目录与促销目录缓存 TTL（1 小时）。
	modelsTTL = 3600 * time.Second
)

// uuid4 生成随机 UUIDv4 字符串（Python str(uuid.uuid4()) 同款形状）。
func uuid4() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", 36)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// UUIDHex 生成 n 个 hex 字符的随机 id（Python uuid4().hex[:n] 同款）。
func UUIDHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

func uuidHex(n int) string { return UUIDHex(n) }

// Client WorkBuddy 上游通道：模型目录/促销缓存（按 realm 分桶）+ 聊天请求。
type Client struct {
	Cred *cred.Store

	// probeDir 可调性结果所在目录：Python 版落在 lanes/（模块所在目录）。
	// Go 版沿用 <状态目录>/lanes，把 exe 放进原 WorkBuddy2api 目录即可
	// 直接接管 Python 版的 models_probe*.json。
	probeDir string

	// BaseOverride 非空时，模型目录与聊天请求都打到该地址（测试注入假上游）。
	BaseOverride string

	chatHTTP  *http.Client // 流式：不设整体 Timeout，靠 context + 逐帧读
	plainHTTP *http.Client // /v3/config、探测等短请求

	// 会话级头在进程内固定（跨请求复用同一会话身份）
	convID     string
	convReqID  string
	modelsLock sync.Mutex // 单飞锁：缓存失效瞬间多个并发只放行一个去拉上游

	mu     sync.Mutex
	models map[string]*bucket // realm -> 模型目录
	promos map[string]*bucket // realm -> 促销目录
}

type bucket struct {
	at     time.Time
	data   []map[string]any // 目录条目；models 桶里 data==nil 表示未加载
	loaded bool
}

// NewClient 建上游通道；stateDir 是网关状态目录（<stateDir>/lanes 放探测缓存，
// 与 Python 版 lanes/models_probe*.json 同路径）。
func NewClient(c *cred.Store, stateDir string) *Client {
	return &Client{
		Cred:     c,
		probeDir: filepath.Join(stateDir, "lanes"),
		// 聊天流不能设 http.Client.Timeout——那是「读完整 body」的总时限，
		// 会把长流式响应掐死；这里只设连接阶段超时，读侧由调用方控制。
		chatHTTP: &http.Client{
			Transport: &http.Transport{
				ResponseHeaderTimeout: upstreamTimeout,
			},
		},
		plainHTTP: &http.Client{Timeout: 30 * time.Second},
		convID:    uuid4(), convReqID: uuid4(),
		models: map[string]*bucket{},
		promos: map[string]*bucket{},
	}
}

// BuildHeaders 官方全量头：会话级稳定 + 请求级逐请求生成。
func (c *Client) BuildHeaders(account cred.Account, token string) http.Header {
	traceID := uuidHex(32)
	spanID := uuidHex(16)
	h := http.Header{}
	set := func(k, v string) { h.Set(k, v) }
	set("Authorization", "Bearer "+token)
	set("Content-Type", "application/json")
	set("Accept", "application/json")
	// 上游按 UA 校验客户端版本：缺少 "CLI/<版本>" 会被拒
	// （实测 400 / code 12403 "check ua, get coding copilot version error"）
	set("User-Agent", c.Cred.UserAgent(account.Realm))
	// 官方强制：SSE 不能被压缩缓冲
	set("Accept-Encoding", "identity")
	set("x-requested-with", "XMLHttpRequest")
	// —— 会话级（须稳定）——
	set("X-Conversation-ID", c.convID)
	set("X-Conversation-Request-ID", c.convReqID)
	// —— 请求级（逐请求生成）——
	set("X-Conversation-Message-ID", uuid4())
	set("X-Request-ID", uuid4())
	set("X-Agent-Intent", "craft")
	set("X-Agent-Purpose", "conversation")
	set("x-codebuddy-request", "1")
	// —— 产品身份 ——
	set("X-Product", "SaaS")
	set("X-IDE-Type", "WorkBuddy")
	set("X-IDE-Name", "WorkBuddy")
	set("X-IDE-Version", AppVersion)
	set("X-Private-Data", "true")
	// —— 账号 ——
	set("X-User-Id", account.UID)
	// —— trace 三件套共享同一 traceId ——
	set("traceparent", fmt.Sprintf("00-%s-%s-01", traceID, spanID))
	set("b3", fmt.Sprintf("%s-%s-1-%s", traceID, spanID, uuidHex(16)))
	set("X-Trace-ID", traceID)
	if account.Domain != "" {
		set("X-Domain", account.Domain)
	}
	return h
}

// listModelsOnce 拉一次 /v3/config，返回 (models, promos)。
func (c *Client) listModelsOnce(realm string) ([]map[string]any, []map[string]any, error) {
	name, err := c.Cred.Resolve(realm)
	if err != nil {
		return nil, nil, err
	}
	account, err := c.Cred.Load(name)
	if err != nil {
		return nil, nil, err
	}
	token, err := c.Cred.Token(name)
	if err != nil {
		return nil, nil, err
	}
	base := c.Cred.APIOrigin(name)
	if c.BaseOverride != "" {
		base = c.BaseOverride // 测试注入假上游
	}
	req, err := http.NewRequest(http.MethodGet, base+configPath, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header = c.BuildHeaders(account, token)
	resp, err := c.plainHTTP.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if resp.StatusCode != 200 {
		return nil, nil, fmt.Errorf("模型目录获取失败 HTTP %d: %s", resp.StatusCode, trunc(string(raw), 200))
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, nil, fmt.Errorf("模型目录返回非 JSON: %s", trunc(string(raw), 200))
	}
	if code, ok := body["code"]; ok && !isZero(code) {
		return nil, nil, fmt.Errorf("模型目录业务错误 code=%v msg=%v", body["code"], body["msg"])
	}
	data, _ := body["data"].(map[string]any)
	models := asMaps(data["models"])
	// 上游会把已停用的模型留在目录里，只挂一个 disabled: true 壳条目
	// （实测 wb-lite = {disabled:true, id, name}，没有 contextWindow / credits）。
	// 不过滤就会从 /v1/models 出去一个选了必挂的条目。在写缓存这一层拦，
	// 而不是在 ToOpenAIModel 上标 callable=False：上游自己已标不可用，
	// 没必要再把它当候选呈现给客户端。
	kept := make([]map[string]any, 0, len(models))
	for _, m := range models {
		if tr, _ := m["disabled"].(bool); !tr {
			kept = append(kept, m)
		}
	}
	return kept, asMaps(data["modelPromotions"]), nil
}

// ListModels 从 /v3/config 拉模型目录 + 促销目录（1 小时缓存，同响应同刷新）。
// 缓存未命中时走单飞锁：仅一个线程去拉上游，其余并发线程阻塞复用同一份结果，
// 防止多并发同时打上游造成节流 502（2026-10-11 日志 r0114–r0119 六并发全 502 根因）。
func (c *Client) ListModels(force bool, realm string) ([]map[string]any, error) {
	name, err := c.Cred.Resolve(realm)
	if err != nil {
		return nil, err
	}
	if !force {
		if data, ok := c.cachedModels(name); ok {
			return data, nil
		}
	}
	c.modelsLock.Lock()
	defer c.modelsLock.Unlock()
	// 二次检查：等锁期间可能已有线程刷新完缓存
	if !force {
		if data, ok := c.cachedModels(name); ok {
			return data, nil
		}
	}
	models, promos, err := c.listModelsOnce(name)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	c.mu.Lock()
	c.models[name] = &bucket{at: now, data: models, loaded: true}
	c.promos[name] = &bucket{at: now, data: promos, loaded: true}
	c.mu.Unlock()
	return models, nil
}

func (c *Client) cachedModels(name string) ([]map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.models[name]
	if !ok || !b.loaded || time.Since(b.at) >= modelsTTL {
		return nil, false
	}
	return b.data, true
}

// Promos 该 realm 已缓存的促销目录（由 ListModels 同响应填充；未加载返回空）。
func (c *Client) Promos(realm string) []map[string]any {
	name, err := c.Cred.Resolve(realm)
	if err != nil {
		name = "cn"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.promos[name]; ok && b.loaded {
		return b.data
	}
	return nil
}

// ── 促销免费判定（北京时间） ──────────────────────────────────────────────

func bjNow() time.Time {
	return time.Now().In(time.FixedZone("CST", 8*3600)) // Asia/Shanghai 无夏令时，固定 UTC+8
}

// promoFree 促销是否=当前时点 0 扣费。返回 (是否免费, 免费窗口串)。
//
// 判定链：enabled → validFrom/validUntil（北京时间日期）→ daily 窗口
// （无 daily = 全天有效；有则须落在任一窗口内，支持跨零点）。
// 只认 discount.factor==0（如 hy3 0x 限时免费、hy4 夜间 0.00x）；
// factor=0.5（deepseek 非高峰 5 折）与 discount=null（纯 badge）都不算免费。
func promoFree(promo map[string]any, now time.Time) (bool, string) {
	if en, _ := promo["enabled"].(bool); !en {
		return false, ""
	}
	disc, _ := promo["discount"].(map[string]any)
	factor, ok := disc["factor"].(float64)
	if !ok || factor != 0 {
		return false, ""
	}
	sched, _ := promo["schedule"].(map[string]any)
	dateStr := now.Format("2006-01-02")
	validFrom := trunc(str(sched["validFrom"]), 10)
	validUntil := trunc(str(sched["validUntil"]), 10)
	if validFrom != "" && dateStr < validFrom {
		return false, ""
	}
	if validUntil != "" && dateStr >= validUntil { // validUntil 为 T00:00 起失效（末日全天仍有效）
		return false, ""
	}
	daily, _ := sched["daily"].([]any)
	if len(daily) == 0 {
		return true, "" // 无窗口约束 = 全天免费（如 hy3-free-trial-202608）
	}
	cur := now.Hour()*60 + now.Minute()
	inAny := false
	for _, wi := range daily {
		w, _ := wi.(map[string]any)
		start, ok1 := hhmmMinutes(str(w["start"]))
		end, ok2 := hhmmMinutes(str(w["end"]))
		if !ok1 || !ok2 {
			continue
		}
		if start <= end {
			if start <= cur && cur < end {
				inAny = true
				break
			}
		} else if cur >= start || cur < end { // 跨零点窗口
			inAny = true
			break
		}
	}
	if !inAny {
		return false, ""
	}
	// 命中即回完整免费时段（元数据给客户端看全 schedule，而非当前命中的片段）
	parts := make([]string, 0, len(daily))
	for _, wi := range daily {
		w, _ := wi.(map[string]any)
		parts = append(parts, str(w["start"])+"-"+str(w["end"]))
	}
	return true, strings.Join(parts, ",")
}

// hhmmMinutes "23:00" -> 1380。
func hhmmMinutes(s string) (int, bool) {
	hs, ms, ok := strings.Cut(s, ":")
	if !ok {
		return 0, false
	}
	h, err1 := strconv.Atoi(strings.TrimSpace(hs))
	m, err2 := strconv.Atoi(strings.TrimSpace(ms))
	if err1 != nil || err2 != nil {
		return 0, false
	}
	return h*60 + m, true
}

// modelFreePromo 汇总该模型命中的所有 0 扣费促销（多窗口以逗号连接）。
func (c *Client) modelFreePromo(mid string, realm string) (bool, string) {
	var windows []string
	seen := map[string]bool{}
	for _, p := range c.Promos(realm) {
		matched := false
		for _, mi := range asList(p["modelIds"]) {
			if str(mi) == mid {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		free, win := promoFree(p, bjNow())
		if free {
			if win == "" {
				win = "全天"
			}
			if !seen[win] {
				seen[win] = true
				windows = append(windows, win)
			}
		}
	}
	if len(windows) == 0 {
		return false, ""
	}
	return true, strings.Join(windows, ",")
}

// ToOpenAIModel 把上游模型对象转成 OpenAI 模型对象（附能力元数据）。
//
// probe 是本账号可调性结果（load_probe 的 {model_id: {...}}）；没有结果时
// callable 为 nil（**未探测**）——上游目录会把本账号调不动的模型也列出来
// （实测 glm-4.6 等直连即 400/11102 service info not found），不能拿目录当可调清单。
func (c *Client) ToOpenAIModel(model map[string]any, probe map[string]any, realm string) map[string]any {
	contextWindow := model["maxInputTokens"]
	maxTokens := model["maxOutputTokens"]
	supportsImages, _ := model["supportsImages"].(bool)
	// 免费面判定只认上游促销目录（modelPromotions）：factor==0 且当前时点
	// （北京时间）落在有效期内才算 0 扣费；目录 credits=x0.00（如有）仅作静态兜底
	creditsRaw := strings.ToLower(strings.TrimSpace(str(model["credits"])))
	staticFree := creditsRaw == "x0.00" || creditsRaw == "x0" || creditsRaw == "$0" || creditsRaw == "0"
	promoFreeFlag, freeWindow := c.modelFreePromo(str(model["id"]), realm)
	callable, callableCode := any(nil), any(nil)
	if probe != nil {
		callable = probe["callable"]
		callableCode = probe["code"]
	}
	return map[string]any{
		"id":                        model["id"],
		"object":                    "model",
		"created":                   0,
		"owned_by":                  orDefault(str(model["vendor"]), "workbuddy"),
		"name":                      orDefault(str(model["name"]), str(model["id"])),
		"description":               firstNonEmptyStr(str(model["descriptionZh"]), str(model["descriptionEn"])),
		"context_window":            contextWindow,
		"context_length":            contextWindow,
		"max_tokens":                maxTokens,
		"max_output_tokens":         maxTokens,
		"supports_images":           supportsImages,
		"supports_vision":           supportsImages,
		"supports_function_calling": boolf(model["supportsToolCall"]),
		"supports_tool_calling":     boolf(model["supportsToolCall"]),
		"supports_reasoning":        boolf(model["supportsReasoning"]),
		// 推理档位候选：与 reasoning_effort 对应，不接上客户端就猜不到合法取值
		"reasoning_efforts": listOrEmpty(asMap(model["reasoning"]), "supportedEfforts"),
		// 上游目录给的可选上下文档位（实测 2026-10：7 个模型新增 600k 档）。
		// **这是目录信息，不是可用入参**：上游聊天体 Request 结构里没有长度字段
		// （实测拿 {} 探 contextLength/context_length/contextWindow 全部 200 静默丢弃）
		// ——所以选了也不会生效。列出来是为了让客户端知道该模型能力边界
		// （context_window 只是默认值），不是宣称可切换。
		"context_lengths": listOrEmpty(asMap(model["contextWindow"]), "supportedLengths"),
		"only_reasoning":  boolf(model["onlyReasoning"]),
		"credits":         model["credits"],
		// 免费面标注（促销 factor==0 命中 / 目录 x0.00 兜底）；恒为 bool
		"is_free": staticFree || promoFreeFlag,
		// 免费窗口（如 "23:00-23:59,0:00-8:00" 或 "全天"）；非免费为 nil
		"free_window":      freeWindowOrNull(promoFreeFlag, freeWindow),
		"input_modalities": inputModalities(supportsImages),
		// null = 未探测；true/false = 最近一次实调结果
		"callable":      callable,
		"callable_code": callableCode,
	}
}

func inputModalities(images bool) []string {
	if images {
		return []string{"text", "image"}
	}
	return []string{"text"}
}

func freeWindowOrNull(free bool, win string) any {
	if !free {
		return nil
	}
	return win
}

// ── 聊天请求 ─────────────────────────────────────────────────────────────

// BuildPayload 把 OpenAI 请求裁剪成上游请求体。
//
// 上游把 body 反序列化进一个固定结构体：结构体外的键被静默丢弃，非法取值一律默许，
// 只有类型不符才报错（docs/workbuddy-24 §5.1）。因此白名单只收**有行为级证据**的键：
//
//	· 已验证生效：max_tokens、tools、tool_choice（仅字符串）、
//	  reasoning_effort（实测能把思维链 429→748 / 0→638）、temperature
//	  （实测 temperature=0.0 高熵题 5/5 同值，1.0 开始出现分歧）。
//	· 在结构体内、但效力未证：presence_penalty / frequency_penalty / user。
//	· **不转发**（结构体外或实测无效，接上就是向客户端宣称假接口）：stop、
//	  response_format、logprobs/top_logprobs、seed、top_k、top_p、
//	  parallel_tool_calls、logit_bias、n。
//	· max_completion_tokens 上游不认（实测设 20 仍跑 277 token），重命名映射到
//	  max_tokens；否则客户端等于没有输出上限。
//
// stream 固定 true（上游不支持非流式，非流式由服务端聚合）。
// tool_choice 上游只收字符串：对象形式一律降 auto——降 required 会在部分后端
// 把原本能跑的请求打成 500（docs/workbuddy-24 §3.7）。
func BuildPayload(req map[string]any) map[string]any {
	payload := map[string]any{
		"model": orDefault(str(req["model"]), "auto"),
		// 注：agent 不在上游聊天端点的结构体内（24 号审计 §5.1），
		// 保留只为与其他官方入口对齐，不应期望它影响历史列表
		"agent":    "cli",
		"stream":   true,
		"messages": listOr(req["messages"]),
	}
	maxTokens, hasMT := req["max_tokens"]
	if !hasMT || maxTokens == nil {
		// OpenAI 已把 max_tokens 标废弃，而上游只认 max_tokens
		maxTokens, hasMT = req["max_completion_tokens"]
	}
	if hasMT && maxTokens != nil {
		payload["max_tokens"] = maxTokens
	}
	for _, key := range []string{"temperature", "presence_penalty", "frequency_penalty", "user", "reasoning_effort"} {
		if v, ok := req[key]; ok && v != nil {
			payload[key] = v
		}
	}
	if tools, ok := req["tools"].([]any); ok && len(tools) > 0 {
		payload["tools"] = tools
	}
	switch choice := req["tool_choice"].(type) {
	case string:
		// 字符串形态不依赖是否带 tools，照转
		payload["tool_choice"] = choice
	case map[string]any:
		// 对象形态上游不收；只能降 auto：降 required 在部分后端直接 500
		if tools, ok := req["tools"].([]any); ok && len(tools) > 0 {
			payload["tool_choice"] = "auto"
		}
	}
	return payload
}

// OpenChatStream 发起上游聊天请求，返回流式响应（body 未读，由调用方消费/关闭）；
// 401 时刷新令牌重试一次。401 的响应体是网关层 HTML（不是 JSON），
// 所以这里只按状态码判断、不看 body。
func (c *Client) OpenChatStream(ctx context.Context, payload map[string]any, account cred.Account) (*http.Response, error) {
	raw, err := marshalCompact(payload)
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxBodyBytes {
		return nil, fmt.Errorf("请求体 %d 字节，超过上游 32MB 上限", len(raw))
	}
	base := c.Cred.APIOrigin(account.Realm)
	if c.BaseOverride != "" {
		base = c.BaseOverride
	}
	url := base + chatPath

	for attempt := 1; ; attempt++ {
		token, err := c.Cred.Token(account.Realm)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		req.Header = c.BuildHeaders(account, token)
		resp, err := c.chatHTTP.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusUnauthorized && attempt == 1 {
			resp.Body.Close()
			// 旧 Bearer 进、新 token 出；刷新端点按 realm 选（国际版同路径、只收 POST）
			if _, err := c.Cred.Refresh(account.Realm); err != nil {
				return nil, err
			}
			continue
		}
		return resp, nil
	}
}

// Aggregate 把上游 SSE 聚合成一份完整 chat.completion（非流式请求用）。
func Aggregate(body io.Reader, model, chunkID string) (map[string]any, error) {
	var content, reasoning []string
	toolCalls := map[int]*toolSlot{}
	usage, finish := any(nil), ""
	err := IterSSE(body, func(kind string, value any) bool {
		switch kind {
		case "content":
			content = append(content, value.(string))
		case "reasoning":
			reasoning = append(reasoning, value.(string))
		case "tool_calls":
			for _, ci := range value.([]any) {
				if call, ok := ci.(map[string]any); ok {
					mergeToolCall(toolCalls, call)
				}
			}
		case "finish":
			finish = value.(string)
		case "usage":
			usage = value
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	message := map[string]any{"role": "assistant", "content": joinOrNone(content)}
	if len(reasoning) > 0 {
		message["reasoning_content"] = strings.Join(reasoning, "")
	}
	calls := sortedCalls(toolCalls)
	if len(calls) > 0 {
		message["tool_calls"] = calls
		message["content"] = nil
	}
	result := map[string]any{
		"id":      chunkID,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index": 0, "message": message,
			"finish_reason": orDefault(finish, defaultFinish(len(calls) > 0)),
		}},
	}
	if usage != nil {
		result["usage"] = usage
	}
	return result, nil
}

func defaultFinish(hasCalls bool) string {
	if hasCalls {
		return "tool_calls"
	}
	return "stop"
}

func joinOrNone(parts []string) any {
	if len(parts) == 0 || strings.Join(parts, "") == "" {
		return nil
	}
	return strings.Join(parts, "")
}

// MakeChunk 构造一帧 OpenAI 流式 chunk；usage 非 nil 时按 include_usage 语义
// 单发空 choices 帧。
func MakeChunk(chunkID, model string, delta map[string]any, finishReason any, usage any) map[string]any {
	if usage != nil {
		return map[string]any{
			"id": chunkID, "object": "chat.completion.chunk",
			"created": time.Now().Unix(), "model": model,
			"choices": []any{}, "usage": usage,
		}
	}
	return map[string]any{
		"id": chunkID, "object": "chat.completion.chunk",
		"created": time.Now().Unix(), "model": model,
		"choices": []any{map[string]any{
			"index": 0, "delta": delta, "finish_reason": finishReason,
		}},
	}
}

// ── 小工具 ───────────────────────────────────────────────────────────────

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func asMaps(v any) []map[string]any {
	l := asList(v)
	if l == nil {
		return nil
	}
	out := make([]map[string]any, 0, len(l))
	for _, e := range l {
		if m, ok := e.(map[string]any); ok && m != nil {
			out = append(out, m)
		}
	}
	return out
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func boolf(v any) bool {
	b, _ := v.(bool)
	return b
}

func isZero(v any) bool {
	if f, ok := v.(float64); ok {
		return f == 0
	}
	return v == nil
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func firstNonEmptyStr(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func listOr(v any) []any {
	if l := asList(v); l != nil {
		return l
	}
	return []any{}
}

func listOrEmpty(m map[string]any, key string) []any {
	if m == nil {
		return []any{}
	}
	if l := asList(m[key]); l != nil {
		return l
	}
	return []any{}
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// marshalCompact JSON 紧凑序列化，不转义 HTML 字符（对齐 Python
// json.dumps(..., ensure_ascii=False, separators=(",",":"))）。
func marshalCompact(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// FetchQuota 查询账号积分额度（官方 /billing/meter/get-user-resource-summary）。
// Python 实测（2026-10-27）：两 realm 均返回 Packages / CreditGroups / TotalCredits
// / SubscriptionPackageName / IsPaidUser；企业口径 get-enterprise-user-usage 对
// 非企业号返回 400，个人号不走那条链路。
func (c *Client) FetchQuota(realm string) (map[string]any, error) {
	account, err := c.Cred.Load(realm)
	if err != nil {
		return nil, err
	}
	base := c.Cred.APIOrigin(realm)
	uid := account.UID
	seed := sha256.Sum256([]byte(uid))
	machine := hex.EncodeToString(seed[:])[:32]
	h := map[string]string{
		"Content-Type": "application/json", "Accept": "application/json, text/plain, */*",
		"X-Requested-With":    "XMLHttpRequest",
		"User-Agent":          "WorkBuddy/5.8.4",
		"Authorization":       "Bearer " + account.Token,
		"X-User-Id":           uid,
		"X-CodeBuddy-Request": "1",
		"X-Request-ID":        fmt.Sprintf("%x", sha256.Sum256([]byte(uid+fmt.Sprint(time.Now().UnixNano()))))[:32],
		"X-Machine-ID":        machine,
		"X-Session-ID":        machine,
		"X-No-Enterprise-Id":  "1",
		"X-Client-Platform":   "web",
	}
	if realm == "intl" {
		h["Origin"] = "https://www.workbuddy.ai"
		h["Referer"] = "https://www.workbuddy.ai/"
		h["X-Domain"] = "www.workbuddy.ai"
		h["Accept-Language"] = "en-US"
	} else {
		h["Origin"] = "https://www.codebuddy.cn"
		h["Referer"] = "https://www.codebuddy.cn/"
		h["X-Domain"] = "copilot.tencent.com"
		h["Accept-Language"] = "zh-CN"
		h["X-Product"] = "SaaS"
	}
	req, err := http.NewRequest(http.MethodPost, base+"/billing/meter/get-user-resource-summary", strings.NewReader("{}"))
	if err != nil {
		return nil, err
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := c.plainHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var envelope struct {
		Code int            `json:"code"`
		Data map[string]any `json:"data"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return nil, fmt.Errorf("HTTP %d 非 JSON: %.120s", resp.StatusCode, string(raw))
	}
	if resp.StatusCode != 200 || envelope.Code != 0 {
		return nil, fmt.Errorf("HTTP %d code=%d", resp.StatusCode, envelope.Code)
	}
	return envelope.Data, nil
}
