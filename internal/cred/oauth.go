// Package cred 的 OAuth 登录：WorkBuddy 自有 state 设备流（不依赖桌面端）。
//
// 协议依据与官方桌面端流量印证（auth/state?platform=CLI、auth/token/refresh 与官方一致）：
//
//	POST {base}/v2/plugin/auth/state?platform=CLI   → { state, authUrl }
//	（浏览器打开 authUrl 登录）
//	GET  {base}/v2/plugin/auth/token?state=<state>  → 未完成 code!=0(pending 11217/12151)
//	                                                   完成 code=0 + token bundle
//	GET  {base}/v2/plugin/login/account?state=...   → { uid, enterpriseId, nickname }
//
// 无 PKCE：安全性依赖服务端签发的一次性 state。准入三件套：Origin/Referer/UA 伪装官方 CLI。
// 凭据落盘位置由 Store 按 realm 决定：cn → auth.json（签到脚本同槽位）、
// intl → auth-intl.json——以前两 realm 共用一份，登一个就把另一个冲掉。
package cred

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	oauthStatePath   = "/v2/plugin/auth/state?platform=CLI"
	oauthTokenPath   = "/v2/plugin/auth/token"
	oauthAccountPath = "/v2/plugin/login/account"
	oauthUACLIF      = "CLI/2.63.2 CodeBuddy/2.63.2"
)

// 轮询 pending 业务码（app.asar loopGetToken 印证：11217/12151 均为未完成）。
var pendingCodes = map[float64]bool{11217: true, 12151: true}

const (
	pollInterval = 2 * time.Second
	pollTimeout  = 300 * time.Second
)

// oauthEndpoints 登录面的 realm 配置：base + Origin（准入三件套之一）。
func oauthEndpoints(realm string) (base, origin string) {
	if NormalizeRealm(realm) == "intl" {
		return GlobalOrigin, "https://www.workbuddy.ai"
	}
	return CNOrigin, "https://www.codebuddy.cn"
}

func oauthHeaders(realm, bearer string) http.Header {
	_, origin := oauthEndpoints(realm)
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "application/json, text/plain, */*")
	h.Set("X-Requested-With", "XMLHttpRequest")
	h.Set("Origin", origin)
	h.Set("Referer", origin+"/")
	h.Set("User-Agent", oauthUACLIF)
	if bearer != "" {
		h.Set("Authorization", "Bearer "+bearer)
	}
	return h
}

// oauthGet 发一次 GET 并按信封纪律解析：HTTP>=300 直接报错，否则返回 body。
func oauthGet(u string, h http.Header) (map[string]any, error) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header = h
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return checkEnvelope(resp.StatusCode, raw)
}

// checkEnvelope 信封纪律：code!=0 一律当错误（返回 pending 码原样供调用方判断）。
func checkEnvelope(status int, raw []byte) (map[string]any, error) {
	var body map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}
	if status >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", status, trunc(fmt.Sprint(body), 200))
	}
	if body == nil {
		body = map[string]any{}
	}
	return body, nil
}

// oauthPost 发一次 JSON POST。
func oauthPost(u string, h http.Header, payload []byte) (map[string]any, error) {
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header = h
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return checkEnvelope(resp.StatusCode, raw)
}

// Login 走完整设备流：state → 浏览器登录 → 轮询 → 账号 → 落盘该 realm 槽位。
// 返回值 {uid, nickname}；打印行为与 Python oauth.py main() 对齐。
func (s *Store) Login(realm string) (map[string]string, error) {
	name := NormalizeRealm(realm)
	if name == "" {
		name = "cn"
	}
	base, _ := oauthEndpoints(name)

	// 1. state + authUrl
	body, err := oauthPost(base+oauthStatePath, oauthHeaders(name, ""), []byte("{}"))
	if err != nil {
		return nil, err
	}
	if code, _ := body["code"].(float64); code != 0 {
		return nil, fmt.Errorf("auth/state 失败: %s", trunc(fmt.Sprint(body), 200))
	}
	step1, _ := body["data"].(map[string]any)
	state := asStr(step1["state"])
	authURL := asStr(step1["authUrl"])
	if state == "" || authURL == "" {
		return nil, fmt.Errorf("auth/state 响应缺少 state/authUrl: %s", trunc(fmt.Sprint(step1), 200))
	}
	fmt.Printf("[oauth] 请在浏览器打开并登录（realm=%s）：\n  %s\n", realm, authURL)

	// 2. 轮询 token
	bundle, err := pollToken(name, state)
	if err != nil {
		return nil, err
	}
	fmt.Println("[oauth] 登录完成，获取账号信息…")

	// 3. 账号信息
	acctURL := base + oauthAccountPath + "?state=" + url.QueryEscape(state)
	body, err = oauthGet(acctURL, oauthHeaders(name, asStr(bundle["accessToken"])))
	if err != nil {
		return nil, err
	}
	if code, _ := body["code"].(float64); code != 0 {
		return nil, fmt.Errorf("login/account 失败: %s", trunc(fmt.Sprint(body), 200))
	}
	account, _ := body["data"].(map[string]any)

	// 4. 落盘
	path, err := s.saveCredentials(bundle, account, name)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[oauth] ✅ 凭据已写入 %s（uid=%s nickname=%s）\n",
		path, first8(asStr(account["uid"])), asStr(account["nickname"]))
	return map[string]string{
		"uid": asStr(account["uid"]), "nickname": asStr(account["nickname"]),
	}, nil
}

// pollToken 轮询 auth/token 直到登录完成，返回 token bundle。
func pollToken(name, state string) (map[string]any, error) {
	base, _ := oauthEndpoints(name)
	u := base + oauthTokenPath + "?state=" + url.QueryEscape(state)
	deadline := time.Now().Add(pollTimeout)
	for time.Now().Before(deadline) {
		body, err := oauthGet(u, oauthHeaders(name, ""))
		if err != nil {
			return nil, err
		}
		code, _ := body["code"].(float64)
		if code == 0 {
			data, _ := body["data"].(map[string]any)
			if asStr(data["accessToken"]) != "" {
				return data, nil
			}
		} else if pendingCodes[code] {
			time.Sleep(pollInterval)
			continue
		} else {
			return nil, fmt.Errorf("auth/token 失败: %s", trunc(fmt.Sprint(body), 200))
		}
		time.Sleep(pollInterval)
	}
	return nil, fmt.Errorf("轮询超时（%.0fs）：浏览器侧未完成登录", pollTimeout.Seconds())
}

// saveCredentials 合并 token + account 写入该 realm 的槽位（cn → auth.json，
// intl → auth-intl.json）。字节格式与 Python 版 json.dump(..., indent=2,
// ensure_ascii=False) 一致，两版可交替读写。
func (s *Store) saveCredentials(bundle, account map[string]any, name string) (string, error) {
	if bundle == nil {
		bundle = map[string]any{}
	}
	if account == nil {
		account = map[string]any{}
	}
	expiresIn, _ := bundle["expiresIn"].(float64)
	domain := asStr(bundle["domain"])
	if domain == "" {
		base, _ := oauthEndpoints(name)
		domain = strings.SplitN(base, "//", 2)[1]
	}
	creds := map[string]any{
		"accessToken":  asStr(bundle["accessToken"]),
		"refreshToken": orEmpty(asStr(bundle["refreshToken"])),
		"expiresAt":    float64(time.Now().UnixMilli()) + expiresIn*1000,
		"uid":          asStr(account["uid"]),
		"nickname":     asStr(account["nickname"]),
		"domain":       domain,
		"_realm":       name,
	}
	for _, key := range []string{"accessToken", "uid"} {
		if asStr(creds[key]) == "" {
			return "", fmt.Errorf("凭据缺少 %s，不落盘", key)
		}
	}
	path := s.StorePath(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	raw, err := marshalIndentNoEscape(creds)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func orEmpty(s string) string { return s }

// marshalIndentNoEscape 产出与 Python json.dump(..., indent=2, ensure_ascii=False)
// 一致的字节：两空格缩进、非 ASCII 不转义、无末尾换行。
// Go 的 json.Marshal 永远转义 < > &（< 等），这里还原回原字符。
func marshalIndentNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func first8(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}
