// Package workbuddy 的可调性探测（目录不等于能调）。
//
// 可调性结果按 realm 分开存：同一个模型在国内外账号上可调性不同
// （上游目录会把本账号调不动的模型也列出来），混存会互相误导。
package workbuddy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	probeTTL    = 24 * time.Hour // 结果一天内有效；可手动重探
	probeText   = "回复一个字：好"
	probeReties = 3
)

// ProbeCachePath 探测结果文件：cn 沿用 Python 版历史路径（lanes/models_probe.json，
// 两版可交替读写），其余 realm 加后缀。
func (c *Client) ProbeCachePath(realm string) string {
	name, err := c.Cred.Resolve(realm)
	if err != nil {
		name = "cn"
	}
	base := filepath.Join(c.probeDir, "models_probe.json")
	if name == "cn" {
		return base
	}
	return filepath.Join(c.probeDir, fmt.Sprintf("models_probe_%s.json", name))
}

// LoadProbe 读该 realm 的可调性缓存 {model_id: {callable, code, msg, at}}；
// 文件缺失/损坏返回空。
func (c *Client) LoadProbe(realm string) map[string]map[string]any {
	raw, err := os.ReadFile(c.ProbeCachePath(realm))
	if err != nil {
		return map[string]map[string]any{}
	}
	var data map[string]map[string]any
	if json.Unmarshal(raw, &data) != nil || data == nil {
		return map[string]map[string]any{}
	}
	return data
}

// ProbeOne 对单模型发一发极小请求，只看能不能调起来（读到首帧即断开，不跑完回复）。
//
// 只用来区分"本账号无此模型服务"（如 400/11102）与"能调"，不能当作能力评测。
// **连接失败不等于不可调**：记 probed=false、callable=nil，不当成结论缓存，
// 下次探测会重跑（否则会把手上一次网络抖动永久误标成该模型不可用）。
func (c *Client) ProbeOne(modelID string, realm string) map[string]any {
	payload := BuildPayload(map[string]any{
		"model": modelID,
		"messages": []any{
			map[string]any{"role": "system", "content": "You are a helpful assistant."},
			map[string]any{"role": "user", "content": probeText},
		},
		"max_tokens": float64(8),
	})
	now := time.Now().Unix()

	// ctx 在函数级持有：探测拿到的是流式 body，函数内 cancel 会掐死还在读的 body
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var resp *http.Response
	var lastErr error
	for attempt := 0; attempt < probeReties; attempt++ {
		account, err := c.Cred.Load(realm)
		if err != nil {
			lastErr = err
		} else {
			resp, lastErr = c.OpenChatStream(ctx, payload, account)
			if lastErr == nil {
				break
			}
		}
		if attempt < probeReties-1 {
			time.Sleep(2 * time.Second)
		}
	}
	if lastErr != nil || resp == nil {
		msg := ""
		if lastErr != nil {
			msg = lastErr.Error()
		}
		return map[string]any{"probed": false, "callable": nil, "code": nil,
			"msg": trunc(msg, 160), "at": now}
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		code, msg := any(nil), ""
		if body != nil {
			code = body["code"]
			msg = str(body["msg"])
		}
		if msg == "" {
			msg = trunc(string(raw), 160)
		}
		return map[string]any{"probed": true, "callable": false, "code": code,
			"msg": trunc(msg, 160), "at": now}
	}
	// 200 即算可调；只读少量字节就关流，避免把回复跑完
	buf := make([]byte, 2048)
	_, _ = resp.Body.Read(buf)
	return map[string]any{"probed": true, "callable": true, "code": nil, "msg": "", "at": now}
}

// ProbeModels 给目录内每个模型跑一次可调性探测，结果落盘。
// 默认用缓存（TTL 1 天）；force=true 重跑全部。返回缓存。
// 这一步会向真实模型发请求（小但非零 credit），所以**不在启动时自动跑**，
// 只由面板/管理员主动触发（POST /admin/api/probe-models）。
func (c *Client) ProbeModels(force bool, realm string) (map[string]map[string]any, error) {
	cache := c.LoadProbe(realm)
	resolved, err := c.Cred.Resolve(realm)
	if err != nil {
		return nil, err
	}
	models, err := c.ListModels(false, realm)
	if err != nil {
		return nil, err
	}
	for _, m := range models {
		mid := str(m["id"])
		if mid == "" {
			continue
		}
		hit := cache[mid]
		// 只复用"已有结论且未过期"的；网络抖动得到的 probed=false 不算结论
		if !force && hit != nil {
			if probed, _ := hit["probed"].(bool); probed {
				at, _ := hit["at"].(float64)
				if time.Since(time.Unix(int64(at), 0)) < probeTTL {
					continue
				}
			}
		}
		res := c.ProbeOne(mid, resolved)
		cache[mid] = res
		c.saveProbe(realm, cache)
	}
	return cache, nil
}

// saveProbe 落盘格式与 Python json.dump(cache, file, ensure_ascii=False, indent=1)
// 对齐（indent=1、非 ASCII 不转义），两版可交替读写。
func (c *Client) saveProbe(realm string, cache map[string]map[string]any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(cache); err != nil {
		return
	}
	// Python json.dump 无末尾换行；Go Encoder.Encode 会补一个 \n
	_ = os.WriteFile(c.ProbeCachePath(realm), bytes.TrimRight(buf.Bytes(), "\n"), 0o600)
}
