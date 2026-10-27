// Package handler 的协议路由与管理员接口，对应 Python 版 routes/chat.py、
// routes/responses.py、routes/anthropic.py 与 server.py 的 /v1/models、
// /admin/api/*。上游发送/重试/错误归一在 server.go，方言翻译在 adapters 包。
package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"workbuddy2api/internal/adapters"
	"workbuddy2api/internal/cred"
	"workbuddy2api/internal/session"
	"workbuddy2api/internal/workbuddy"
)

func uuid24() string { return workbuddy.UUIDHex(24) }

// chatContext 一次请求的公共解析：鉴权、realm、body、上游打开。
type chatContext struct {
	info       reqInfo
	account    cred.Account
	resp       *http.Response
	model      string
	chunkID    string
	wantStream bool
}

// ── /v1/models ─────────────────────────────────────────────────────────────

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !s.authorized(r) {
		s.denyAuth(w)
		return
	}
	realm, err := s.requestRealm(r)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{
			"message": err.Error(), "type": "upstream_error",
		}})
		return
	}
	list, err := s.WB.ListModels(false, realm)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{
			"message": err.Error(), "type": "upstream_error",
		}})
		return
	}
	// 带上本账号可调性结果（未探测的模型返回 callable=null，叫面板/管理员跑探测）
	probe := s.WB.LoadProbe(realm)
	data := make([]any, 0, len(list))
	for _, m := range list {
		if asString(m["id"]) == "" {
			continue
		}
		data = append(data, s.WB.ToOpenAIModel(m, probe[asString(m["id"])], realm))
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// ── /v1/chat/completions ───────────────────────────────────────────────────

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	info := reqInfoFrom(r)
	if !s.authorized(r) {
		s.denyAuth(w)
		return
	}
	body := info.payload
	if msgs, ok := body["messages"].([]any); !ok || len(msgs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{
			"message": "invalid request: 'messages' is required",
			"type":    "invalid_request_error",
		}})
		return
	}
	cc, ok := s.openUpstreamOnce(w, r, info, body)
	if !ok {
		return
	}
	defer cc.resp.Body.Close()

	if cc.wantStream {
		wantUsage := false
		if so, ok := body["stream_options"].(map[string]any); ok {
			wantUsage, _ = so["include_usage"].(bool)
		}
		s.streamFrames(w, r, info,
			s.chatSSELines(cc.resp.Body, cc.model, cc.chunkID, wantUsage), cc.model)
		return
	}
	result, err := workbuddy.Aggregate(cc.resp.Body, cc.model, cc.chunkID)
	if err != nil {
		s.failUpstream(w, info, err)
		return
	}
	out := marshalCompact(result)
	s.finishNonStream(info, out)
	writeJSONBody(w, out)
}

// ── /v1/responses ──────────────────────────────────────────────────────────

func (s *Server) responses(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	info := reqInfoFrom(r)
	if !s.authorized(r) {
		s.denyAuth(w)
		return
	}
	if len(info.payload) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{
			"message": "invalid request: JSON object body required",
			"type":    "invalid_request_error",
		}})
		return
	}
	// previous_response_id / conversation：从网关本地会话里取历史前置
	prevID := asString(info.payload["previous_response_id"])
	convID := ""
	if c := info.payload["conversation"]; c != nil {
		if cm, ok := c.(map[string]any); ok {
			convID = asString(cm["id"])
		} else {
			convID = asString(c)
		}
	}
	var history []any
	if s.Sessions != nil {
		history = s.Sessions.Chain(prevID, convID)
	}
	chatBody := adapters.ResponsesToChatWithHistory(info.payload, history)
	// Python routes/responses.py：翻译后无 messages → 400（Responses 空 input 同错）
	if msgs, ok := chatBody["messages"].([]any); !ok || len(msgs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{
			"message": "invalid request: 'input' is required",
			"type":    "invalid_request_error",
		}})
		return
	}
	cc, ok := s.openUpstreamOnce(w, r, info, chatBody)
	if !ok {
		return
	}
	defer cc.resp.Body.Close()

	if cc.wantStream {
		lines := framesToLines(s.chatSSELines(cc.resp.Body, cc.model, cc.chunkID, false))
		// Err 访问器恒 nil：上游读错误已在 chatSSELines 内部转成错误帧
		s.streamFrames(w, r, info,
			adapters.ChatSSEToResponses(adapters.SSELines{Seq: lines, Err: noErr}, cc.model), cc.model)
		return
	}
	result, err := workbuddy.Aggregate(cc.resp.Body, cc.model, cc.chunkID)
	if err != nil {
		s.failUpstream(w, info, err)
		return
	}
	maxCalls := 0
	if v, ok := info.payload["max_tool_calls"]; ok {
		if f, ok2 := v.(float64); ok2 {
			maxCalls = int(f)
		}
	}
	out := marshalCompact(adapters.ChatJSONToResponsesLimited(result, cc.model, maxCalls))
	s.finishNonStream(info, out)
	// store:true（默认 true）时保存会话，供 previous_response_id / conversation 续接
	s.saveResponseSession(info.payload, out, cc.model)
	writeJSONBody(w, out)
}

// saveResponseSession 把一次 Responses 响应存进本地会话（store 默认 true，
// 与官方一致；显式 store:false 则不存）。
func (s *Server) saveResponseSession(req map[string]any, out []byte, model string) {
	if s.Sessions == nil {
		return
	}
	if store, ok := req["store"].(bool); ok && !store {
		return
	}
	var resp map[string]any
	if json.Unmarshal(out, &resp) != nil {
		return
	}
	id := asString(resp["id"])
	if id == "" {
		return
	}
	convID := ""
	if c := req["conversation"]; c != nil {
		if cm, ok := c.(map[string]any); ok {
			convID = asString(cm["id"])
		} else {
			convID = asString(c)
		}
	}
	outItems, _ := resp["output"].([]any)
	inItems, _ := req["input"].([]any)
	if inItems == nil {
		if str, ok := req["input"].(string); ok && str != "" {
			inItems = []any{map[string]any{"role": "user", "content": str}}
		}
	}
	usage, _ := resp["usage"].(map[string]any)
	s.Sessions.Save(&session.Record{
		ID: id, Conversation: convID, Input: inItems, Output: outItems,
		Model: model, Usage: usage,
	})
}

// ── /v1/messages（Anthropic）───────────────────────────────────────────────

func (s *Server) anthropicMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	info := reqInfoFrom(r)
	if !s.authorized(r) {
		s.denyAuth(w)
		return
	}
	if len(info.payload) == 0 {
		writeJSON(w, http.StatusBadRequest, adapters.AnthropicErrorBody(
			"invalid_request_error", "invalid request: JSON object body required"))
		return
	}
	chatBody := adapters.AnthropicToChat(info.payload)
	if msgs, ok := chatBody["messages"].([]any); !ok || len(msgs) == 0 {
		writeJSON(w, http.StatusBadRequest, adapters.AnthropicErrorBody(
			"invalid_request_error", "invalid request: 'messages' is required"))
		return
	}
	cc, ok := s.openUpstreamOnce(w, r, info, chatBody)
	if !ok {
		return
	}
	defer cc.resp.Body.Close()

	if cc.wantStream {
		lines := framesToLines(s.chatSSELines(cc.resp.Body, cc.model, cc.chunkID, false))
		s.streamFrames(w, r, info,
			adapters.ChatSSEToAnthropic(lines, cc.model), cc.model)
		return
	}
	result, err := workbuddy.Aggregate(cc.resp.Body, cc.model, cc.chunkID)
	if err != nil {
		s.failUpstream(w, info, err)
		return
	}
	out := marshalCompact(adapters.ChatJSONToAnthropic(result, cc.model))
	s.finishNonStream(info, out)
	writeJSONBody(w, out)
}

// ── 上游打开公共段 ─────────────────────────────────────────────────────────

// openUpstreamOnce 解析 realm/凭据、裁剪 payload、带上游（含瞬态重试）、
// 处理非 200。返回 ok=false 时响应已写出。
func (s *Server) openUpstreamOnce(w http.ResponseWriter, r *http.Request, info reqInfo,
	body map[string]any) (*chatContext, bool) {

	realm, err := s.requestRealm(r)
	if err != nil {
		// Python 版：未知 realm 在 try 块内抛出，与凭据失败同路 → 502 upstream_error
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{
			"message": err.Error(), "type": "upstream_error",
		}})
		return nil, false
	}
	account, err := s.Cred.Load(realm)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{
			"message": err.Error(), "type": "upstream_error",
		}})
		return nil, false
	}
	payload := workbuddy.BuildPayload(body)
	model := asString(payload["model"])
	wantStream, _ := body["stream"].(bool)

	resp, err := s.openUpstream(r.Context(), info.rid, payload, account)
	if err != nil {
		s.failUpstream(w, info, err)
		return nil, false
	}
	if resp.StatusCode != http.StatusOK {
		errBody, status := s.upstreamError(info.rid, resp)
		writeJSON(w, status, errBody)
		return nil, false
	}
	return &chatContext{
		info: info, account: account, resp: resp,
		model: model, chunkID: "chatcmpl-" + uuid24(), wantStream: wantStream,
	}, true
}

func (s *Server) failUpstream(w http.ResponseWriter, info reqInfo, err error) {
	s.Log.Error("上游不可用", slog.String("rid", info.rid), slog.String("err", err.Error()))
	writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{
		"message": err.Error(), "type": "upstream_error",
	}})
}

func (s *Server) finishNonStream(info reqInfo, out []byte) {
	s.doneLog(info.rid, "status=200", len(out), info.t0)
	s.Dump.Save(info.rid, "resp", string(out))
}

func writeJSONBody(w http.ResponseWriter, out []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// noErr adapters 的读错误访问器：上游读错误已在 chatSSELines 内部转成错误帧，
// 到 adapters 这一层恒为 nil。
func noErr() error { return nil }

// ── 管理面板 API ───────────────────────────────────────────────────────────

func (s *Server) adminOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !s.authorized(r) {
		s.denyAuth(w)
		return
	}
	realm, _ := s.requestRealm(r)

	info := map[string]any{
		"realm": "", "realm_label": "", "uid": "", "nickname": "", "domain": "",
		"valid": false, "remain_text": "—", "notice": "",
	}
	account, err := s.Cred.Load(realm)
	if err != nil {
		info["notice"] = err.Error()
	} else {
		info["realm"] = account.Realm
		info["realm_label"] = account.RealmLabel
		info["uid"] = account.UID
		info["nickname"] = account.Nickname
		info["domain"] = account.Domain
		info["valid"] = account.Token != ""
		info["remain_text"] = cred.RemainText(account.ExpiresAt)
		realm = account.Realm
	}

	models := make([]any, 0)
	probe := s.WB.LoadProbe(realm)
	list, err := s.WB.ListModels(false, realm)
	if err != nil {
		if notice, _ := info["notice"].(string); notice == "" {
			info["notice"] = err.Error()
		}
	} else {
		for _, m := range list {
			id := asString(m["id"])
			if id == "" {
				continue
			}
			var efforts []any
			if rm, ok := m["reasoning"].(map[string]any); ok {
				if l, ok := rm["supportedEfforts"].([]any); ok {
					efforts = l
				}
			}
			if efforts == nil {
				efforts = []any{}
			}
			callable := any(nil)
			if p, ok := probe[id]; ok {
				callable = p["callable"]
			}
			tools, _ := m["supportsToolCall"].(bool)
			images, _ := m["supportsImages"].(bool)
			models = append(models, map[string]any{
				"id": id, "name": firstNonEmpty(asString(m["name"]), id),
				"vendor": asString(m["vendor"]), "credits": m["credits"],
				"tools": tools, "images": images,
				"efforts": efforts, "callable": callable,
			})
		}
	}

	probed, unavailable := 0, 0
	for _, mi := range models {
		m := mi.(map[string]any)
		if m["callable"] != nil {
			probed++
			if b, ok := m["callable"].(bool); ok && !b {
				unavailable++
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"service":    "WorkBuddy2api",
		"credential": info,
		// 两个槽位都列出来：国际版/国内版各一份凭据，缺哪个一眼可见
		"realms":     s.Cred.Status(),
		"models":     models,
		"local_auth": s.Cfg.APIKey != "",
		"probe": map[string]any{"done": probed, "total": len(models),
			"unavailable": unavailable},
		// 提醒：usage.completion_tokens_details.reasoning_tokens 在 WorkBuddy 上恒为 0，
		// 真实思维链量在 usage.completion_thinking_tokens
		"usage_note": "reasoning_tokens 恒为 0；真实思维链量看 completion_thinking_tokens",
	})
}

func (s *Server) adminRefreshToken(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !s.authorized(r) {
		s.denyAuth(w)
		return
	}
	realm, _ := s.requestRealm(r)
	if _, err := s.Cred.Refresh(realm); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"ok": false, "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "令牌已刷新"})
}

// adminProbeModels 逐模型实发一发极小请求，标出本账号到底哪些模型调得动。
// 会向真实模型发请求（每个 max_tokens=8，credit 小但非零），所以只给手动触发。
func (s *Server) adminProbeModels(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !s.authorized(r) {
		s.denyAuth(w)
		return
	}
	force := false
	if body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)); err == nil {
		var j map[string]any
		if json.Unmarshal(body, &j) == nil && j != nil {
			force, _ = j["force"].(bool)
		}
	}
	realm, _ := s.requestRealm(r)
	cache, err := s.WB.ProbeModels(force, realm)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": err.Error()})
		return
	}
	ok := []string{}
	bad := map[string]any{}
	unknown := []string{}
	for k, v := range cache {
		if c, _ := v["callable"].(bool); c {
			ok = append(ok, k)
		}
		if probed, _ := v["probed"].(bool); probed {
			if c, _ := v["callable"].(bool); !c {
				bad[k] = v["code"]
			}
		} else {
			unknown = append(unknown, k)
		}
	}
	s.Log.Info("[probe] 可调性探测完成",
		slog.Int("callable", len(ok)), slog.Int("unavailable", len(bad)),
		slog.Int("undecided", len(unknown)))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "callable": len(ok), "unavailable": bad, "undecided": unknown,
		"message": fmt.Sprintf("可调 %d 个，不可调 %d 个，未判定 %d 个（网络抖动不计结论）",
			len(ok), len(bad), len(unknown)),
	})
}
