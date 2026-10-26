// golden_test.go httptest 假上游测试：鉴权、三协议流/非流、上游错误归一、
// realm 切换、瞬态重试。假上游经 workbuddy.Client.BaseOverride 注入。
package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"workbuddy2api/internal/config"
	"workbuddy2api/internal/cred"
	"workbuddy2api/internal/dump"
	"workbuddy2api/internal/workbuddy"
)

// ── 假上游 ──────────────────────────────────────────────────────────────────

// chatSSEUp 常量上游 SSE：两段 content + finish + usage + [DONE]
// （WorkBuddy 上游 = OpenAI chunk 形状；reasoning 与 content 同帧时
// IterSSE 不会重复累加，这里分开帧更直观）。
var chatSSEUp = strings.Join([]string{
	`data: {"choices":[{"delta":{"role":"assistant","content":"你"}}]}`,
	``,
	`data: {"choices":[{"delta":{"content":"好"}}]}`,
	``,
	`data: {"choices":[{"delta":{"reasoning_content":"想"}}]}`,
	``,
	`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
	``,
	`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2,"completion_thinking_tokens":1}}`,
	``,
	`data: [DONE]`,
	``,
}, "\n")

// captured 一次上游请求的关键形状（断言用）。
type captured struct {
	path        string
	auth        string
	uid         string
	ua          string
	body        map[string]any
	count       int
	modelReqs   int
	realmHeader string
}

// newEnv 建完整测试环境：cn 槽位假凭据 + httptest 假上游（upFn 自定义行为）。
// 返回 server handler 与捕获器。
func newEnv(t *testing.T, upFn func(w http.ResponseWriter, r *http.Request, capr *captured)) (*env, *captured) {
	t.Helper()
	dir := t.TempDir()
	capr := &captured{}

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capr.count++
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		capr.path = r.URL.Path
		capr.auth = r.Header.Get("Authorization")
		capr.uid = r.Header.Get("X-User-Id")
		capr.ua = r.Header.Get("User-Agent")
		capr.body = payload
		upFn(w, r, capr)
	}))
	t.Cleanup(up.Close)

	// cn 槽位凭据（expiresAt 未过、_realm 显式标记）
	future := fmt.Sprint(time.Now().Add(24 * time.Hour).UnixMilli())
	authJSON := `{"accessToken":"tok-cn-local","refreshToken":"ref-cn-local","expiresAt":` + future + `,"uid":"u-123","nickname":"tester","domain":"copilot.tencent.com","enterpriseId":"e1","_realm":"cn"}`
	credStore := cred.NewStore(dir)
	if err := os.WriteFile(filepath.Join(dir, "auth.json"), []byte(authJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	wb := workbuddy.NewClient(credStore, dir)
	wb.BaseOverride = up.URL

	cfg := &config.Config{
		Host: "127.0.0.1", Port: 0, StateDir: dir,
		DumpOn: false, DumpDir: filepath.Join(dir, "dumps"), DumpMaxFiles: 10,
		PanelPath: filepath.Join(dir, "panel.html"),
	}
	srv := &Server{
		Cfg: cfg, Cred: credStore, WB: wb,
		Dump: dump.New(false, cfg.DumpDir, 10),
		Log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return &env{t: t, srv: srv, cred: credStore, cfg: cfg}, capr
}

type env struct {
	t    *testing.T
	srv  *Server
	cred *cred.Store
	cfg  *config.Config
}

// postJSON 直连 handler 发 POST。
func (e *env) postJSON(path, body string, header http.Header) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	e.srv.Handler().ServeHTTP(rec, req)
	return rec
}

func (e *env) get(path string, header http.Header) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	e.srv.Handler().ServeHTTP(rec, req)
	return rec
}

// sseFrames 把流式响应体拆成 data: JSON 帧列表（[DONE] 以外）。
func sseFrames(t *testing.T, body string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(payload), &m); err != nil {
			t.Fatalf("坏 SSE 帧 %q: %v", payload, err)
		}
		out = append(out, m)
	}
	return out
}

// ── 测试 ────────────────────────────────────────────────────────────────────

func TestHealth(t *testing.T) {
	env, _ := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		t.Error("health 不应打上游")
	})
	rec := env.get("/health", nil)
	if rec.Code != 200 {
		t.Fatalf("health = %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("health 非 JSON: %s", rec.Body.String())
	}
	if body["ok"] != true || body["service"] != "WorkBuddy2api" {
		t.Fatalf("health 字段缺失: %v", body)
	}
	if body["realm"] != "cn" {
		t.Fatalf("默认 realm = %v，要 cn", body["realm"])
	}
	if realms, ok := body["realms"].([]any); !ok || len(realms) != 2 {
		t.Fatalf("realms 应列出两个槽位: %v", body["realms"])
	}
}

func TestAPIKeyGuard(t *testing.T) {
	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		// 能到这里的就是带对 Bearer 的第二个请求（401 短路请求绝不打上游，
		// 由下方 capr.count==1 断言兜住）
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, chatSSEUp)
	})
	env.cfg.APIKey = "secret-key"
	rec := env.postJSON("/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("无 Bearer = %d，要 401", rec.Code)
	}
	// health 免鉴权
	if rec := env.get("/health", nil); rec.Code != 200 {
		t.Fatalf("health 应免鉴权，得 %d", rec.Code)
	}
	// 对 Bearer 放行（假上游照常答 200，非流式聚合）
	rec = env.postJSON("/v1/chat/completions", `{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`,
		http.Header{"Authorization": []string{"Bearer secret-key"}})
	if rec.Code != 200 {
		t.Fatalf("对 Bearer = %d: %s", rec.Code, rec.Body.String())
	}
	if capr.count != 1 {
		t.Fatalf("上游被打 %d 次，要 1（401 请求不计）", capr.count)
	}
}

func TestChatRequiresMessages(t *testing.T) {
	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		t.Error("校验失败不应打上游")
	})
	rec := env.postJSON("/v1/chat/completions", `{"model":"wb-pro"}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("= %d，要 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "'messages' is required") {
		t.Fatalf("错误消息不对: %s", rec.Body.String())
	}
	if capr.count != 0 {
		t.Fatalf("上游被打了 %d 次", capr.count)
	}
}

func TestChatNonStream(t *testing.T) {
	env, _ := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, chatSSEUp)
	})
	rec := env.postJSON("/v1/chat/completions", `{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != 200 {
		t.Fatalf("= %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("非 JSON: %s", rec.Body.String())
	}
	if body["object"] != "chat.completion" || body["model"] != "wb-pro" {
		t.Fatalf("形状不对: %v", body)
	}
	choices := body["choices"].([]any)
	ch := choices[0].(map[string]any)
	msg := ch["message"].(map[string]any)
	if msg["content"] != "你好" {
		t.Fatalf("content = %v，要 你好", msg["content"])
	}
	if msg["reasoning_content"] != "想" {
		t.Fatalf("reasoning_content = %v，要 想", msg["reasoning_content"])
	}
	if ch["finish_reason"] != "stop" {
		t.Fatalf("finish_reason = %v", ch["finish_reason"])
	}
	usage := body["usage"].(map[string]any)
	if usage["prompt_tokens"] != float64(5) || usage["completion_tokens"] != float64(2) {
		t.Fatalf("usage = %v", usage)
	}
	// thinking_tokens 必须原样透出（WorkBuddy 的真实思维链量在补丁字段）
	if usage["completion_thinking_tokens"] != float64(1) {
		t.Fatalf("completion_thinking_tokens = %v，要 1", usage["completion_thinking_tokens"])
	}
}

func TestChatUpstreamShape(t *testing.T) {
	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, chatSSEUp)
	})
	rec := env.postJSON("/v1/chat/completions",
		`{"model":"wb-pro","stream":true,"max_completion_tokens":77,"stop":"x","messages":[{"role":"user","content":"hi"}]}`,
		http.Header{"Authorization": []string{"Bearer client-key"}})
	if rec.Code != 200 {
		t.Fatalf("= %d: %s", rec.Code, rec.Body.String())
	}
	// 上游形状：POST /v2/chat/completions、Bearer 是凭据 token 而非客户端 key
	if capr.path != "/v2/chat/completions" {
		t.Fatalf("上游 path = %s", capr.path)
	}
	if capr.auth != "Bearer tok-cn-local" {
		t.Fatalf("上游 Authorization = %s", capr.auth)
	}
	if capr.uid != "u-123" {
		t.Fatalf("X-User-Id = %s", capr.uid)
	}
	// UA 按段校验：缺 "CLI/<版本>" 上游 400
	if !strings.Contains(capr.ua, "WorkBuddy/") || !strings.Contains(capr.ua, "CLI/") {
		t.Fatalf("上游 UA = %s", capr.ua)
	}
	if capr.body["model"] != "wb-pro" {
		t.Fatalf("model = %v", capr.body["model"])
	}
	// stream 固定 true（上游不支持非流式）
	if capr.body["stream"] != true {
		t.Fatalf("stream = %v，要 true", capr.body["stream"])
	}
	// max_completion_tokens 重命名映射 max_tokens；stop 白名单外不转发
	if capr.body["max_tokens"] != float64(77) {
		t.Fatalf("max_tokens = %v，要 77（max_completion_tokens 映射）", capr.body["max_tokens"])
	}
	if _, ok := capr.body["stop"]; ok {
		t.Fatalf("stop 不在上游白名单内，却转发了: %v", capr.body["stop"])
	}
}

func TestChatStreamReshape(t *testing.T) {
	env, _ := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, chatSSEUp)
	})
	rec := env.postJSON("/v1/chat/completions",
		`{"model":"wb-pro","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != 200 {
		t.Fatalf("= %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %s", ct)
	}
	frames := sseFrames(t, rec.Body.String())
	if len(frames) < 4 {
		t.Fatalf("帧数太少: %d", len(frames))
	}
	// 首帧带 role（OpenAI 惯例），且内容帧只在前两帧
	first := frames[0]["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if first["role"] != "assistant" || first["content"] != "你" {
		t.Fatalf("首帧 delta = %v，要 role=assistant + content=你", first)
	}
	// finish 与 usage 拆帧：usage 帧空 choices（include_usage 未要时内联收尾帧）
	var sawFinish, sawUsage bool
	var content strings.Builder
	for _, f := range frames {
		choices, _ := f["choices"].([]any)
		for _, ci := range choices {
			ch := ci.(map[string]any)
			if d, ok := ch["delta"].(map[string]any); ok {
				if s, ok := d["content"].(string); ok {
					content.WriteString(s)
				}
			}
			if fr, _ := ch["finish_reason"].(string); fr == "stop" {
				sawFinish = true
			}
		}
		if _, ok := f["usage"].(map[string]any); ok {
			sawUsage = true
		}
	}
	if content.String() != "你好" {
		t.Fatalf("content 聚合 = %q，要 你好", content.String())
	}
	if !sawFinish {
		t.Fatalf("没有 finish 帧: %v", frames)
	}
	if !sawUsage {
		t.Fatalf("usage 应内联在收尾帧（客户端没要 include_usage）: %v", frames)
	}
	// 收尾必须是 [DONE]
	if !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Fatalf("缺 [DONE] 收尾")
	}
}

func TestResponsesInputRequired(t *testing.T) {
	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		t.Error("空 input 不应打上游")
	})
	rec := env.postJSON("/v1/responses", `{"model":"wb-pro"}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("= %d，要 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "'input' is required") {
		t.Fatalf("错误消息不对: %s", rec.Body.String())
	}
	if capr.count != 0 {
		t.Fatalf("上游被打了 %d 次", capr.count)
	}
}

func TestResponsesNonStream(t *testing.T) {
	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, chatSSEUp)
	})
	// Responses 的 input 字符串翻译成 chat messages
	rec := env.postJSON("/v1/responses", `{"model":"wb-pro","stream":false,"input":"hi"}`, nil)
	if rec.Code != 200 {
		t.Fatalf("= %d: %s", rec.Code, rec.Body.String())
	}
	if msgs, ok := capr.body["messages"].([]any); !ok || len(msgs) != 1 {
		t.Fatalf("input 未翻译成 messages: %v", capr.body["messages"])
	}
	var body map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("非 JSON: %s", rec.Body.String())
	}
	if body["object"] != "response" {
		t.Fatalf("object = %v，要 response", body["object"])
	}
	// output 里应有 assistant 文本
	out, _ := body["output"].([]any)
	if len(out) == 0 {
		t.Fatalf("output 为空: %s", rec.Body.String())
	}
}

func TestAnthropicNonStream(t *testing.T) {
	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, chatSSEUp)
	})
	rec := env.postJSON("/v1/messages",
		`{"model":"wb-pro","max_tokens":64,"system":"be nice","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != 200 {
		t.Fatalf("= %d: %s", rec.Code, rec.Body.String())
	}
	// system 翻译成首条 system role 消息
	msgs := capr.body["messages"].([]any)
	first := msgs[0].(map[string]any)
	if first["role"] != "system" {
		t.Fatalf("system 未翻译成首条 system 消息: %v", msgs)
	}
	var body map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("非 JSON: %s", rec.Body.String())
	}
	if body["type"] != "message" || body["role"] != "assistant" {
		t.Fatalf("Anthropic 形状不对: %v", body)
	}
	blocks, _ := body["content"].([]any)
	if len(blocks) == 0 {
		t.Fatalf("content 为空: %s", rec.Body.String())
	}
	block := blocks[0].(map[string]any)
	if block["type"] != "text" || block["text"] != "你好" {
		t.Fatalf("首个 content block = %v，要 text/你好", block)
	}
	if _, ok := body["usage"].(map[string]any); !ok {
		t.Fatalf("缺 usage: %s", rec.Body.String())
	}
}

func TestUpstreamErrorJSON(t *testing.T) {
	// 500 属瞬态名单，会吃满 4 次重试；退避压到毫秒级别让测试睡 14 秒
	old := upstreamRetryBackoff
	upstreamRetryBackoff = [3]time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	defer func() { upstreamRetryBackoff = old }()

	env, _ := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"code":1201,"msg":"模型过载","displayMsg":{"zh":"稍后再试"}}`)
	})
	rec := env.postJSON("/v1/chat/completions", `{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("= %d，要 500 透传", rec.Code)
	}
	var body map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("非 JSON: %s", rec.Body.String())
	}
	errObj := body["error"].(map[string]any)
	if errObj["type"] != "workbuddy_error" {
		t.Fatalf("type = %v，要 workbuddy_error", errObj["type"])
	}
	if errObj["message"] != "模型过载（稍后再试）" {
		t.Fatalf("message = %v，要 msg+displayMsg.zh", errObj["message"])
	}
	if errObj["code"] != float64(1201) {
		t.Fatalf("code = %v", errObj["code"])
	}
}

func TestUpstreamErrorNonJSON(t *testing.T) {
	old := upstreamRetryBackoff
	upstreamRetryBackoff = [3]time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	defer func() { upstreamRetryBackoff = old }()

	env, _ := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, `<html>502 Bad Gateway</html>`)
	})
	rec := env.postJSON("/v1/chat/completions", `{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("= %d，要 502", rec.Code)
	}
	var body map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("非 JSON: %s", rec.Body.String())
	}
	errObj := body["error"].(map[string]any)
	// 网关层 HTML 不能原样吐给客户端，归一成 upstream_error
	if errObj["type"] != "upstream_error" || !strings.Contains(fmt.Sprint(errObj["message"]), "上游网关拦截") {
		t.Fatalf("error = %v", errObj)
	}
}

func TestTransientRetry(t *testing.T) {
	// 重试退避改成毫秒级，别让测试睡 14 秒
	old := upstreamRetryBackoff
	upstreamRetryBackoff = [3]time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	defer func() { upstreamRetryBackoff = old }()

	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, c *captured) {
		if c.count == 1 {
			// 第一次 503 排队（瞬态），第二次 200
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"code":1,"msg":"queue.full"}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, chatSSEUp)
	})
	rec := env.postJSON("/v1/chat/completions", `{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != 200 {
		t.Fatalf("瞬态 503 后应重试成功，得 %d: %s", rec.Code, rec.Body.String())
	}
	if capr.count < 2 {
		t.Fatalf("上游只被打了 %d 次，要 ≥2", capr.count)
	}
}

func TestRealmSwitch(t *testing.T) {
	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, chatSSEUp)
	})
	// 未知 realm → 502 upstream_error（报错而不是静默回落）
	rec := env.postJSON("/v1/chat/completions",
		`{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`,
		http.Header{"X-WorkBuddy-Realm": []string{"bogus"}})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("未知 realm = %d，要 502", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "未知 realm") {
		t.Fatalf("错误消息不对: %s", rec.Body.String())
	}
	// intl 槽位无凭据 → 502 带登录指引
	rec = env.postJSON("/v1/chat/completions",
		`{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`,
		http.Header{"X-WorkBuddy-Realm": []string{"intl"}})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("intl 无凭据 = %d，要 502", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "国际版") || !strings.Contains(rec.Body.String(), "login") {
		t.Fatalf("intl 提示不对: %s", rec.Body.String())
	}
	// 正常请求（cn）不受影响
	rec = env.postJSON("/v1/chat/completions", `{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != 200 {
		t.Fatalf("cn = %d: %s", rec.Code, rec.Body.String())
	}
	if capr.count != 1 {
		t.Fatalf("上游被打 %d 次，要 1", capr.count)
	}
}

func TestModels(t *testing.T) {
	env, _ := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		if r.URL.Path != "/v3/config" {
			t.Errorf("models 应打 /v3/config，得 %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"code":0,"data":{"models":[
			{"id":"wb-lite","name":"WB Lite","disabled":true},
			{"id":"wb-pro","name":"WB Pro","vendor":"tencent","credits":2,"supportsToolCall":true,"supportsImages":false}
		],"modelPromotions":[]}}`)
	})
	rec := env.get("/v1/models", nil)
	if rec.Code != 200 {
		t.Fatalf("= %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("非 JSON: %s", rec.Body.String())
	}
	if body["object"] != "list" {
		t.Fatalf("object = %v", body["object"])
	}
	data := body["data"].([]any)
	// disabled 壳条目必须在目录层拦掉
	if len(data) != 1 {
		t.Fatalf("models = %d 条，要 1（disabled 拦掉）: %s", len(data), rec.Body.String())
	}
	m := data[0].(map[string]any)
	if m["id"] != "wb-pro" {
		t.Fatalf("id = %v", m["id"])
	}
}

func TestUpstream400NotRetried(t *testing.T) {
	// 401 会触发令牌刷新重试一次；400 非瞬态也不重试
	old := upstreamRetryBackoff
	upstreamRetryBackoff = [3]time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	defer func() { upstreamRetryBackoff = old }()

	env, capr := newEnv(t, func(w http.ResponseWriter, r *http.Request, _ *captured) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"code":12403,"msg":"check ua, get coding copilot version error"}`)
	})
	rec := env.postJSON("/v1/chat/completions", `{"model":"wb-pro","stream":false,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("= %d，要 400 透传", rec.Code)
	}
	if capr.count != 1 {
		t.Fatalf("非瞬态 400 不应重试，上游被打 %d 次", capr.count)
	}
	errObj := mustJSON(t, rec.Body.Bytes())["error"].(map[string]any)
	if errObj["message"] != "check ua, get coding copilot version error" {
		t.Fatalf("message = %v", errObj["message"])
	}
}

func mustJSON(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("非 JSON: %s", raw)
	}
	return m
}
