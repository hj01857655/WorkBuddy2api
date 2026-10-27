// Package handler 对应 Python 版 server.py + routes/：路由调度、鉴权闸门、rid、
// in:/done: 日志、请求落盘、CORS、realm 选择、上游重试与错误归一。
// 上游协议细节在 workbuddy 包，方言翻译在 adapters 包。
package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"workbuddy2api/internal/session"

	"workbuddy2api/internal/config"
	"workbuddy2api/internal/cred"
	"workbuddy2api/internal/dump"
	"workbuddy2api/internal/workbuddy"
)

// 上游瞬态失败重试（与 Python 版一致）：限免模型高峰容易排队（429 queue.waiting /
// queue.full），上游偶尔还会直接掐 TLS 连接——两者都是"时间"问题，退避重试；
// 401 已由 OpenChatStream 内部刷新令牌重试一次，其余 4xx 非瞬态，原样返回。
const upstreamMaxAttempts = 4

var upstreamRetryBackoff = [3]time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}

var transientStatus = map[int]bool{429: true, 500: true, 502: true, 503: true, 504: true}

// Server 聚合全部依赖；Handler 挂到 http.ServeMux。
type Server struct {
	Cfg  *config.Config
	Cred *cred.Store
	WB   *workbuddy.Client
	Dump *dump.Writer
	Log  *slog.Logger
	// Sessions Responses API 的本地会话（previous_response_id / conversation / store）
	Sessions *session.Store

	reqSeq atomic.Int64
}

// logSkip 面板与探活不进请求日志（Python before/after_request 的 path 名单）。
var logSkip = map[string]bool{"/": true, "/admin": true, "/health": true, "/favicon.ico": true}

// nextRID 单调递增请求号（r%04d；进程内单调，dump 文件名带启动段防跨进程撞名）。
func (s *Server) nextRID() string {
	return fmt.Sprintf("r%04d", s.reqSeq.Add(1))
}

// Handler 组装路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/v1/models", s.models)
	mux.HandleFunc("/v1/chat/completions", s.chat)
	mux.HandleFunc("/v1/responses", s.responses)
	mux.HandleFunc("/v1/messages", s.anthropicMessages)
	mux.HandleFunc("/", s.panelOr404)
	mux.HandleFunc("/admin", s.panelOr404)
	mux.HandleFunc("/admin/api/overview", s.adminOverview)
	mux.HandleFunc("/admin/api/refresh-token", s.adminRefreshToken)
	mux.HandleFunc("/admin/api/probe-models", s.adminProbeModels)
	mux.HandleFunc("/admin/api/quota", s.adminQuota)
	return s.observe(mux)
}

// observe 对应 before_request：rid/t0 在最早的环节赋值（401 短路也要有 done 行可关联）、
// in: 日志、req 原文落盘；再加全局 CORS 头（Python 是 after_request 统一加，
// 这里在响应头层面等价）。鉴权闸门保持 per-route（health 与面板不鉴权）。
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := s.nextRID()
		t0 := time.Now()

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

		var payload map[string]any
		bodyLen := 0
		var rawBody []byte
		if !logSkip[r.URL.Path] {
			if r.Body != nil {
				rawBody, _ = io.ReadAll(io.LimitReader(r.Body, 64<<20))
				bodyLen = len(rawBody)
				if len(rawBody) > 0 {
					_ = json.Unmarshal(rawBody, &payload)
				}
			}
			s.logIn(rid, r, payload, bodyLen)
			s.Dump.Save(rid, "req", string(rawBody))
		}

		ctx := withReqInfo(r.Context(), reqInfo{rid: rid, t0: t0, payload: payload})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authorized 判定当前请求是否通过本地鉴权。
// 未设 API_KEY 时仅监听回环、不校验；设了就要求 Bearer 匹配（只拼一次前缀）。
func (s *Server) authorized(r *http.Request) bool {
	if s.Cfg.APIKey == "" {
		return true
	}
	return r.Header.Get("Authorization") == "Bearer "+s.Cfg.APIKey
}

func (s *Server) denyAuth(w http.ResponseWriter) {
	writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{
		"message": "invalid api key", "type": "auth_error",
	}})
}

// requestRealm 本次请求用哪个 realm（国内版 cn / 国际版 intl）。
//
// 优先级：请求头 X-WorkBuddy-Realm > 查询参数 ?realm= > 环境变量 WB_REALM
// > 有凭据的槽位（cn 优先）。返回空串表示交给 cred 自己决定。
// 两个 realm 就用同一个端口切换，不必起两份服务。
// raw 非空但不认识时报错（未知 realm 应当报错而不是静默回落）。
func (s *Server) requestRealm(r *http.Request) (string, error) {
	raw := r.Header.Get("X-WorkBuddy-Realm")
	if raw == "" {
		raw = r.URL.Query().Get("realm")
	}
	name := cred.NormalizeRealm(raw)
	if raw != "" && name == "" {
		return "", fmt.Errorf("未知 realm: %s（可用 cn / intl）", raw)
	}
	return name, nil
}

// logIn 出 in: 行（rid/t0 与 body 摘要 + UA）。
// UA 是日志里唯一的客户端归因维度（API key 全客户端共用、ip 恒为 127.0.0.1）；
// 折掉换行防伪造多行日志。
func (s *Server) logIn(rid string, r *http.Request, payload map[string]any, bodyLen int) {
	ua := r.Header.Get("User-Agent")
	if ua == "" {
		ua = "-"
	}
	ua = strings.ReplaceAll(strings.ReplaceAll(ua, "\r", " "), "\n", " ")
	summary := "n/a"
	if payload != nil {
		var msgs, tools int
		if l, ok := payload["messages"].([]any); ok {
			msgs = len(l)
		}
		if l, ok := payload["tools"].([]any); ok {
			tools = len(l)
		}
		summary = fmt.Sprintf("model=%v msgs=%d tools=%d stream=%v",
			payload["model"], msgs, tools, payload["stream"])
	}
	s.Log.Info("in:",
		slog.String("rid", rid),
		slog.String("line", fmt.Sprintf("%s %s | %s | ua=%s | body=%dB",
			r.Method, r.URL.Path, summary, ua, bodyLen)),
	)
}

func (s *Server) doneLog(rid, detail string, bytes int, t0 time.Time) {
	s.Log.Info("done:",
		slog.String("rid", rid),
		slog.String("line", fmt.Sprintf("%s %dB %.1fs", detail, bytes, time.Since(t0).Seconds())),
	)
}

// ── 上游通道 ───────────────────────────────────────────────────────────────

// openUpstream OpenChatStream + 瞬态失败重试。
// 返回 open 的响应（200 或最终失败状态码）；连接层失败耗尽重试时
// 错误原样返回（调用方统一 502）。
func (s *Server) openUpstream(ctx context.Context, rid string, payload map[string]any, account cred.Account) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < upstreamMaxAttempts; attempt++ {
		resp, err := s.WB.OpenChatStream(ctx, payload, account)
		if err != nil {
			lastErr = err
			if attempt < upstreamMaxAttempts-1 {
				s.Log.Warn("上游连接失败，稍后重试",
					slog.String("rid", rid),
					slog.String("line", fmt.Sprintf("%.0fs 后重试 %d/%d: %s",
						upstreamRetryBackoff[attempt].Seconds(), attempt+1, upstreamMaxAttempts-1, err)))
				time.Sleep(upstreamRetryBackoff[attempt])
				continue
			}
			return nil, err
		}
		if resp.StatusCode == 200 || !transientStatus[resp.StatusCode] {
			return resp, nil
		}
		if attempt < upstreamMaxAttempts-1 {
			resp.Body.Close()
			s.Log.Warn("上游排队/5xx，稍后重试",
				slog.String("rid", rid),
				slog.String("line", fmt.Sprintf("HTTP %d，%.0fs 后重试 %d/%d",
					resp.StatusCode, upstreamRetryBackoff[attempt].Seconds(), attempt+1, upstreamMaxAttempts-1)))
			time.Sleep(upstreamRetryBackoff[attempt])
			continue
		}
		return resp, nil
	}
	return nil, lastErr
}

// tokenPeekRe 抹掉日志里的长 token 样串（≥24 位 base64/hex 形态），避免凭据落入日志。
var tokenPeekRe = regexp.MustCompile(`[A-Za-z0-9_\-]{24,}`)

// upstreamError 把上游错误转成 OpenAI error 结构（对应 _upstream_error）。
//
// 401 等网关层拦截返回的是 HTML，**绝不能原样吐给客户端**——统一成 502 + JSON。
// 扁平码族 {code,msg,displayMsg} 里的 code 是通用码，语义看 msg。
// 返回 (payload, httpStatus)。
func (s *Server) upstreamError(rid string, resp *http.Response) (map[string]any, int) {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	text := string(raw)
	if len(text) > 2000 {
		text = text[:2000]
	}
	ctype := strings.ToLower(resp.Header.Get("Content-Type"))
	// 落原因到日志：否则外层只留一行 "done: status=502 287B"，
	// 上游到底是拦凭证、限流还是改路由根本分不出来（归一化会抹掉原始状态码）。
	// 只留 200 字并抹掉长 token 样串。
	peek := strings.Join(strings.Fields(text), " ")
	peek = tokenPeekRe.ReplaceAllString(peek, "…")
	if len(peek) > 200 {
		peek = peek[:200]
	}
	s.Log.Warn("上游非 200",
		slog.String("rid", rid),
		slog.String("line", fmt.Sprintf("HTTP %d ctype=%s %s", resp.StatusCode, orDash(ctype), peek)))

	if strings.Contains(ctype, "json") {
		var body map[string]any
		if json.Unmarshal(raw, &body) == nil && body != nil {
			message := firstNonEmpty(asString(body["msg"]), asString(body["message"]), text)
			if display, ok := body["displayMsg"].(map[string]any); ok {
				if zh := asString(display["zh"]); zh != "" {
					message = fmt.Sprintf("%s（%s）", message, zh)
				}
			}
			return map[string]any{"error": map[string]any{
				"message": message,
				"type":    "workbuddy_error",
				"code":    body["code"],
			}}, resp.StatusCode
		}
	}

	return map[string]any{"error": map[string]any{
		"message": fmt.Sprintf("上游网关拦截（HTTP %d，非 JSON 响应）", resp.StatusCode),
		"type":    "upstream_error",
		"code":    resp.StatusCode,
	}}, http.StatusBadGateway
}

func orDash(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

// ── Chat SSE 管线 ──────────────────────────────────────────────────────────

// chatSSELines 产出 Chat SSE 原始帧（带 data: 前缀，含 [DONE] 收尾帧）。
// chat 与 responses/anthropic 路由共用：后两者把这帧流再包成各自协议的 SSE。
//
// 与 Python _chat_sse_lines 对齐：上游每帧都带 delta.role，但 OpenAI 惯例是
// 只首帧带角色，客户端靠它初始化 message（与 stream=false 的 message.role 对齐）。
// 上游读错误时补一条错误帧再收尾（Python 版直接让异常掀掉响应流，客户端只见半截）。
func (s *Server) chatSSELines(body io.Reader, model, chunkID string, wantUsage bool) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		events, errFn := workbuddy.SSESeq(body)
		finishReason, sawTools, sentRole := "", false, false
		var usage any

		send := func(frame map[string]any) bool {
			raw, err := json.Marshal(frame)
			if err != nil {
				return true // 坏帧跳过，不中断整条流（对齐 Python 坏帧跳过）
			}
			return yield(append(append([]byte("data: "), raw...), '\n', '\n'))
		}

		for ev := range events {
			var delta map[string]any
			switch ev.Kind {
			case "content":
				delta = map[string]any{"content": ev.Value}
			case "reasoning":
				delta = map[string]any{"reasoning_content": ev.Value}
			case "tool_calls":
				sawTools = true
				delta = map[string]any{"tool_calls": ev.Value}
			case "finish":
				finishReason, _ = ev.Value.(string)
				continue
			case "usage":
				usage = ev.Value
				continue
			default:
				continue
			}
			frame := workbuddy.MakeChunk(chunkID, model, delta, nil, nil)
			if !sentRole {
				first := frame["choices"].([]any)[0].(map[string]any)
				d := first["delta"].(map[string]any)
				role := map[string]any{"role": "assistant"}
				for k, v := range d {
					role[k] = v
				}
				first["delta"] = role
				sentRole = true
			}
			if !send(frame) {
				return
			}
		}

		if err := errFn(); err != nil {
			s.Log.Error("stream 中断（上游读错误）", slog.String("err", err.Error()))
			// 客户端把半截回答当完整结果比裸掐断更难排查，显式补错误帧
			errFrame := map[string]any{"error": map[string]any{
				"message": "upstream stream ended prematurely: " + err.Error(),
				"type":    "upstream_error",
			}}
			if !send(errFrame) {
				return
			}
		}

		finish := finishReason
		if finish == "" {
			finish = "tool_calls"
			if !sawTools {
				finish = "stop"
			}
		}
		tail := workbuddy.MakeChunk(chunkID, model, map[string]any{}, finish, nil)
		if usage != nil && !wantUsage {
			tail["usage"] = usage // 客户端没要独立 usage 帧时内联在收尾帧
		}
		if !send(tail) {
			return
		}
		if usage != nil && wantUsage {
			if !send(workbuddy.MakeChunk(chunkID, model, map[string]any{}, nil, usage)) {
				return
			}
		}
		yield([]byte("data: [DONE]\n\n"))
	}
}

// framesToLines 把 Chat 帧流拆成行流（喂 adapters 的 SSELines 用）。
// 每帧形如 "data: {...}\n\n"，按 \n 切分即可；空行不影响 adapters 解析。
func framesToLines(frames iter.Seq[[]byte]) iter.Seq[string] {
	return func(yield func(string) bool) {
		for frame := range frames {
			trimmed := strings.TrimRight(string(frame), "\n")
			sc := bufio.NewScanner(strings.NewReader(trimmed))
			for sc.Scan() {
				if !yield(sc.Text()) {
					return
				}
			}
			if !yield("") {
				return
			}
		}
	}
}

// ── 请求上下文 ─────────────────────────────────────────────────────────────

type reqInfo struct {
	rid     string
	t0      time.Time
	payload map[string]any
}

type reqInfoKey struct{}

func withReqInfo(ctx context.Context, info reqInfo) context.Context {
	return context.WithValue(ctx, reqInfoKey{}, info)
}

func reqInfoFrom(r *http.Request) reqInfo {
	info, _ := r.Context().Value(reqInfoKey{}).(reqInfo)
	return info
}

// ── 响应辅助 ───────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(marshalCompact(v))
}

// marshalCompact JSON 紧凑序列化，不转义 HTML 字符（中文原样输出，
// 对齐 Python json.dumps(..., ensure_ascii=False, separators=(",",":"))）。
func marshalCompact(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return []byte("null")
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// sseHeaders 流式响应头（Cache-Control/X-Accel-Buffering 与 Python 版一致）。
func sseHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
}

// streamFrames 帧流 → 客户端：TTFT、字节数、原文落盘、done/断开/中断日志
// （Python _stream_log 的等价物）。
func (s *Server) streamFrames(w http.ResponseWriter, r *http.Request, info reqInfo,
	frames iter.Seq[[]byte], kind string) {
	sseHeaders(w)
	flusher, _ := w.(http.Flusher)
	total := 0
	ttft := ""
	var dumpBuf []byte
	for frame := range frames {
		if err := r.Context().Err(); err != nil {
			s.Log.Warn("client 断开", slog.String("rid", info.rid),
				slog.String("line", fmt.Sprintf("stream %dB %.1fs", total, time.Since(info.t0).Seconds())))
			return
		}
		if ttft == "" {
			ttft = fmt.Sprintf("%.1fs", time.Since(info.t0).Seconds())
		}
		if s.Dump.On && len(dumpBuf) <= 512*1024 {
			dumpBuf = append(dumpBuf, frame...)
		}
		total += len(frame)
		if _, werr := w.Write(frame); werr != nil {
			s.Log.Warn("client 断开", slog.String("rid", info.rid),
				slog.String("line", fmt.Sprintf("stream %dB %.1fs", total, time.Since(info.t0).Seconds())))
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	if len(dumpBuf) > 0 {
		s.Dump.Save(info.rid, "resp", string(dumpBuf))
	}
	detail := fmt.Sprintf("stream %s", kind)
	if ttft != "" {
		detail += " ttft=" + ttft
	}
	s.doneLog(info.rid, detail, total, info.t0)
}

// ── 探活 / 面板 ────────────────────────────────────────────────────────────

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	// realm 与槽位状态放进探活：客户端拿不准现在走哪套上游时，先看这里
	active, notice := "", ""
	realm, err := s.requestRealm(r)
	if err != nil {
		notice = err.Error()
	} else if name, err := s.Cred.Resolve(realm); err != nil {
		notice = err.Error()
	} else {
		active = name
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "service": "WorkBuddy2api",
		"realm": active, "realms": s.Cred.Status(), "notice": notice,
	})
}

// panelOr404 / 与 /admin 送面板；其余未知路径 404（Flask 未注册路由的行为）。
func (s *Server) panelOr404(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/admin" {
		http.NotFound(w, r)
		return
	}
	s.servePanel(w)
}

// ── 日志：request.log 2MB×3 轮转 + stdout（与 Python RotatingFileHandler 对齐）──

// NewLogger 建 slog logger：同时写 stdout 与 request.log（2MB 轮转，保 3 份）。
// werkzeug 访问行在响应*开始*时打（流式会误导，且与 done 行重复）——Go 版
// 没有默认访问日志，天然对齐。
func NewLogger(stateDir string) *slog.Logger {
	path := filepath.Join(stateDir, "request.log")
	rw := &rotateWriter{path: path, maxBytes: 2 << 20, backupCount: 3}
	return slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, rw), nil))
}

type rotateWriter struct {
	mu          sync.Mutex
	path        string
	maxBytes    int64
	backupCount int
	f           *os.File
	written     int64
}

func (rw *rotateWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if rw.f == nil {
		if err := rw.open(); err != nil {
			return 0, err
		}
	}
	if rw.written+int64(len(p)) > rw.maxBytes {
		rw.rotate()
	}
	n, err := rw.f.Write(p)
	rw.written += int64(n)
	return n, err
}

func (rw *rotateWriter) open() error {
	f, err := os.OpenFile(rw.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	rw.f = f
	if info, err := f.Stat(); err == nil {
		rw.written = info.Size()
	}
	return nil
}

// rotate 关闭当前文件，.log.(n-1) → .log.n 链式改名，重开新文件。
func (rw *rotateWriter) rotate() {
	if rw.f != nil {
		rw.f.Close()
		rw.f = nil
	}
	for i := rw.backupCount - 1; i > 0; i-- {
		os.Rename(fmt.Sprintf("%s.%d", rw.path, i), fmt.Sprintf("%s.%d", rw.path, i+1))
	}
	os.Rename(rw.path, rw.path+".1")
	rw.written = 0
	_ = rw.open()
}

func (s *Server) adminQuota(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{"ok": true, "realms": map[string]any{}}
	realms := out["realms"].(map[string]any)
	for _, realm := range []string{"intl", "cn"} {
		data, err := s.WB.FetchQuota(realm)
		if err != nil {
			realms[realm] = map[string]any{"ok": false, "error": err.Error()}
			continue
		}
		realms[realm] = map[string]any{"ok": true, "data": data}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(out)
}
