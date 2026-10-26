// Package checkin 对应 Python 版 lanes/checkin.py：WorkBuddy 每日签到。
//
// 设计要点（照抄 Python 版）：
//  1. 凭据引导一次，之后自续：从桌面端本地登录态取 refreshToken，换取自己的
//     accessToken/refreshToken 并存入本仓库（auth.json）；之后每天用 refresh 自续。
//  2. 只读桌面端文件，绝不改写：桌面端数据归它自己管。
//  3. 日志与终端输出永不含令牌原文。
//  4. 幂等：code=0 成功；msg 含「已签到/already」视为已签（按成功处理）。
//
// 端点（实测）：
//
//	签到：POST {domain}/v2/billing/meter/daily-checkin      body {}
//	状态：POST {domain}/v2/billing/meter/checkin-status
//	续期：POST {domain}/v2/plugin/auth/token/refresh
//
// 必需请求头（缺 X-User-Id 必 401）。续期额外带 X-Refresh-Token + X-Auth-Refresh-Source。
package checkin

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	epClaim   = "/v2/billing/meter/daily-checkin"
	epStatus  = "/v2/billing/meter/checkin-status"
	epRefresh = "/v2/plugin/auth/token/refresh"

	codeOK = float64(0)
	// 10001 是**通用**业务错误码，不是「已签到」专用码：实测同一码对应两种语义，
	// 必须靠 msg 区分（「今天已签到」= 幂等成功；「活动未开启/已过期」= 真失败）。
	codeAlready = float64(10001)

	refreshAheadSec = 6 * 3600 // 剩余不足 6 小时就续期
	reqTimeout      = 25 * time.Second

	browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
)

// Cred 本仓库凭据（auth.json），字段与 Python 版/checkin 脚本一致。
type Cred struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    int64  `json:"expiresAt"`
	UID          string `json:"uid"`
	Nickname     string `json:"nickname"`
	Domain       string `json:"domain"`
	EnterpriseID string `json:"enterpriseId"`

	src string // 引导来源文件（doctor 打印用，不落盘）
}

// Store 凭据仓库；目录默认 ~/.workbuddy/checkin（测试可重定向）。
type Store struct {
	Dir      string
	LogFn    func(string)
	noProxy  bool
	uidPref  string
	logFile  string
	infoDirs []string // 桌面端登录态目录（只读扫描）
}

// NewStore 用默认目录与桌面端扫描路径建仓库。
func NewStore() *Store {
	home, _ := os.UserHomeDir()
	dir := filepath.Join(home, ".workbuddy", "checkin")
	s := &Store{Dir: dir, logFile: filepath.Join(dir, "checkin.log")}
	s.infoDirs = desktopAuthDirs()
	s.LogFn = s.defaultLog
	return s
}

func desktopAuthDirs() []string {
	var out []string
	for _, env := range []string{"LOCALAPPDATA", "APPDATA"} {
		base := os.Getenv(env)
		if base == "" {
			continue
		}
		out = append(out, filepath.Join(base, "CodeBuddyExtension", "Data", "Public", "auth"))
	}
	return out
}

func (s *Store) authStore() string { return filepath.Join(s.Dir, "auth.json") }

func (s *Store) defaultLog(msg string) {
	line := fmt.Sprintf("[%s] %s", time.Now().Format("2006-01-02 15:04:05"), msg)
	fmt.Println(line)
	_ = os.MkdirAll(s.Dir, 0o700)
	if f, err := os.OpenFile(s.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err == nil {
		_, _ = f.WriteString(line + "\n")
		f.Close()
	}
}

func (s *Store) log(msg string) {
	if s.LogFn != nil {
		s.LogFn(msg)
	} else {
		s.defaultLog(msg)
	}
}

// ── HTTP ───────────────────────────────────────────────────────────────────

func (s *Store) post(u string, headers map[string]string, body []byte) (int, map[string]any) {
	if body == nil {
		body = []byte("{}")
	}
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return -1, map[string]any{"_err": err.Error()}
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: reqTimeout}
	if s.noProxy {
		client.Transport = &http.Transport{Proxy: nil}
	}
	resp, err := client.Do(req)
	if err != nil {
		return -1, map[string]any{"_err": fmt.Sprintf("%s:%s", "http", trunc(err.Error(), 100))}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var j map[string]any
	if err := json.Unmarshal(raw, &j); err != nil {
		if len(raw) == 0 {
			j = map[string]any{}
		} else {
			j = map[string]any{"_raw": trunc(string(raw), 200)}
		}
	}
	return resp.StatusCode, j
}

func apiHeaders(c *Cred) map[string]string {
	h := map[string]string{
		"Authorization":    "Bearer " + c.AccessToken,
		"Accept":           "application/json, text/plain, */*",
		"Content-Type":     "application/json",
		"X-User-Id":        c.UID,
		"X-Requested-With": "XMLHttpRequest",
		"Origin":           "https://" + c.Domain,
		"Referer":          "https://" + c.Domain + "/",
		"User-Agent":       browserUA,
		"X-Domain":         c.Domain,
	}
	if c.EnterpriseID != "" {
		h["X-Enterprise-Id"] = c.EnterpriseID
		h["X-Tenant-Id"] = c.EnterpriseID
	}
	return h
}

// ── 凭据仓库读写 ────────────────────────────────────────────────────────────

func (s *Store) loadStore() *Cred {
	raw, err := os.ReadFile(s.authStore())
	if err != nil {
		return nil
	}
	var c Cred
	if json.Unmarshal(raw, &c) != nil {
		return nil
	}
	if c.RefreshToken != "" && c.UID != "" && c.Domain != "" {
		return &c
	}
	return nil
}

// saveStore 写回 auth.json（indent=2、非 ASCII 不转义——与 Python json.dump 一致）。
func (s *Store) saveStore(c *Cred) string {
	_ = os.MkdirAll(s.Dir, 0o700)
	raw, err := marshalIndent2(c)
	if err != nil {
		return "写回失败"
	}
	if err := os.WriteFile(s.authStore(), raw, 0o600); err != nil {
		return "写回失败"
	}
	return hardenPermissions(s.authStore())
}

// ── 桌面端引导（只读） ──────────────────────────────────────────────────────

// activeAccountUID 读主文件 workbuddy-desktop.info 的 account.uid —— 桌面端当前登录账号。
// 目录里可能同时有多个账号的明文备份，只有当前账号才是用户想签的那个。
func (s *Store) activeAccountUID() string {
	for _, d := range s.infoDirs {
		p := filepath.Join(d, "workbuddy-desktop.info")
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var j map[string]any
		if json.Unmarshal(b, &j) != nil {
			continue
		}
		acct, _ := j["account"].(map[string]any)
		if uid := asStr(acct["uid"]); uid != "" {
			return uid
		}
	}
	return ""
}

// bootstrapFromDesktop 从桌面端本地登录态取**明文** refreshToken。
// 实测新版桌面端（5.6.2）把令牌包成 {$wbEncrypted, envelope} 加密形态，取不到，
// 只能靠带时间戳的明文备份文件；故扫描目录下全部 .info，只接受明文者。
// 账号优先级：显式 uid 前缀 > 主文件当前账号 > 按 expiresAt 取最新
// （不要只用 expiresAt 排序：不同账号令牌寿命不同，会选错号）。
func (s *Store) bootstrapFromDesktop() *Cred {
	prefix := s.uidPref
	if prefix == "" {
		prefix = s.activeAccountUID()
	}
	var cands []*Cred
	for _, d := range s.infoDirs {
		entries, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".info") {
				continue
			}
			path := filepath.Join(d, e.Name())
			b, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var j map[string]any
			if json.Unmarshal(b, &j) != nil {
				continue
			}
			auth, _ := j["auth"].(map[string]any)
			acct, _ := j["account"].(map[string]any)
			rt, rtOK := auth["refreshToken"].(string)
			if !rtOK || rt == "" {
				continue // 加密形态或无 refreshToken，跳过
			}
			uid := asStr(acct["uid"])
			if prefix != "" && !strings.HasPrefix(uid, prefix) {
				continue
			}
			at, _ := auth["accessToken"].(string)
			dom := asStr(auth["domain"])
			if dom == "" {
				dom = "www.workbuddy.cn"
			}
			exp, _ := auth["expiresAt"].(float64)
			cands = append(cands, &Cred{
				UID: uid, Nickname: asStr(acct["nickname"]), Domain: dom,
				AccessToken: at, RefreshToken: rt, ExpiresAt: int64(exp),
				EnterpriseID: asStr(acct["enterpriseId"]), src: path,
			})
		}
	}
	if len(cands) == 0 {
		return nil
	}
	// 按 expiresAt 降序取最新
	var best *Cred
	for _, c := range cands {
		if best == nil || c.ExpiresAt > best.ExpiresAt {
			best = c
		}
	}
	return best
}

// parseJWTExp 从 JWT payload 取 exp（秒）；非 JWT 返回 0。
func parseJWTExp(tok string) int64 {
	parts := strings.Split(tok, ".")
	if len(parts) < 2 {
		return 0
	}
	seg := parts[1]
	if pad := len(seg) % 4; pad != 0 {
		seg += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(seg)
	if err != nil {
		return 0
	}
	var p map[string]any
	if json.Unmarshal(raw, &p) != nil {
		return 0
	}
	if exp, ok := p["exp"].(float64); ok {
		return int64(exp)
	}
	return 0
}

// DoRefresh 用 refreshToken 换新令牌，成功则写回仓库。
func (s *Store) DoRefresh(c *Cred) bool {
	h := apiHeaders(c)
	delete(h, "Authorization")
	h["X-Refresh-Token"] = c.RefreshToken
	h["X-Auth-Refresh-Source"] = "plugin"
	status, j := s.post("https://"+c.Domain+epRefresh, h, []byte(""))
	code, _ := j["code"].(float64)
	if status != 200 || code != codeOK {
		s.log(fmt.Sprintf("续期失败：HTTP %d code=%v msg=%v",
			status, j["code"], firstNonEmpty(asStr(j["msg"]), asStr(j["_err"]))))
		return false
	}
	data, _ := j["data"].(map[string]any)
	if asStr(data["accessToken"]) == "" {
		s.log("续期响应缺少 accessToken")
		return false
	}
	c.AccessToken = asStr(data["accessToken"])
	if rt := asStr(data["refreshToken"]); rt != "" {
		c.RefreshToken = rt
	}
	if d := asStr(data["domain"]); d != "" {
		c.Domain = d
	}
	exp := parseJWTExp(c.AccessToken)
	left := ""
	if exp != 0 {
		c.ExpiresAt = exp * 1000
		left = fmt.Sprintf("，有效期至 %s",
			time.Unix(exp, 0).UTC().Format("2006-01-02"))
	}
	desc := s.saveStore(c)
	s.log(fmt.Sprintf("续期成功%s（凭据已存 %s）", left, desc))
	return true
}

// ResolveCred 取可用凭据：自有仓库 → 临近过期则续期 → 从桌面端引导。
func (s *Store) ResolveCred() *Cred {
	c := s.loadStore()
	if c != nil {
		if c.ExpiresAt != 0 && float64(c.ExpiresAt)/1000-float64(time.Now().Unix()) < refreshAheadSec {
			s.log(fmt.Sprintf("本地凭据将于 %s 过期，先续期",
				time.Unix(c.ExpiresAt/1000, 0).UTC().Format("2006-01-02 15:04 UTC")))
			if !s.DoRefresh(c) {
				c = nil
			}
		}
		if c != nil {
			return c
		}
	}
	boot := s.bootstrapFromDesktop()
	if boot == nil {
		s.log("桌面端未找到可用登录态。原因通常是：新版令牌为 envelope 加密形态，" +
			"而目录里没有带明文 refreshToken 的备份文件。")
		s.log("请打开 WorkBuddy 桌面端确认已登录（或先重新登录一次），再重试。")
		return nil
	}
	s.log(fmt.Sprintf("从桌面端引导：%s（uid=%s…）", boot.src, first8(boot.UID)))
	if !s.DoRefresh(boot) {
		s.log("引导到的 refreshToken 已失效，请重新登录桌面端。")
		return nil
	}
	return boot
}

// ── 子命令 ─────────────────────────────────────────────────────────────────

// Claim 签到（幂等），返回退出码（0 成功/已签，非 0 失败）。
func (s *Store) Claim() int {
	c := s.ResolveCred()
	if c == nil {
		return 2
	}
	status, j := s.post("https://"+c.Domain+epClaim, apiHeaders(c), nil)
	if status == -1 {
		s.log("请求失败：" + asStr(j["_err"]))
		return 1
	}
	code, _ := j["code"].(float64)
	msg := asStr(j["msg"])
	if code == codeOK {
		data, _ := j["data"].(map[string]any)
		streak, _ := data["is_streak_day"].(bool)
		tail := ""
		if streak {
			tail = "（连续奖励日）"
		}
		s.log(fmt.Sprintf("✅ 签到成功：+%v 积分，连续 %v 天%s",
			orDash(data["credit"]), orDash(data["streak_days"]), tail))
		return 0
	}
	low := strings.ToLower(msg)
	if strings.Contains(msg, "已签到") || strings.Contains(low, "already") {
		s.log(fmt.Sprintf("ℹ 今天已签到过（code=%v，msg=%s），无需重复操作", j["code"], msg))
		return 0
	}
	if strings.Contains(msg, "未开启") || strings.Contains(msg, "已过期") {
		s.log(fmt.Sprintf("✗ 该账号无可用签到活动：%s（uid=%s… domain=%s）",
			msg, first8(c.UID), c.Domain))
		s.log("  若你有多个账号，用 --uid <前缀> 指定要签的那个。")
		return 1
	}
	s.log(fmt.Sprintf("✗ 签到失败：HTTP %d code=%v msg=%s", status, j["code"],
		firstNonEmpty(msg, asStr(j["_raw"]))))
	return 1
}

// Status 查询签到状态（人类可读；json=true 时返回原始 data）。
func (s *Store) Status(asJSON bool) (int, map[string]any) {
	c := s.ResolveCred()
	if c == nil {
		return 2, nil
	}
	status, j := s.post("https://"+c.Domain+epStatus, apiHeaders(c), nil)
	code, _ := j["code"].(float64)
	if status != 200 || code != codeOK {
		s.log(fmt.Sprintf("状态查询异常：HTTP %d %s", status, trunc(mustJSON(j), 200)))
		return 1, nil
	}
	data, _ := j["data"].(map[string]any)
	if asJSON {
		return 0, data
	}
	dots := ""
	for _, x := range asList(data["week_progress"]) {
		if b, _ := x.(bool); b {
			dots += "●"
		} else {
			dots += "○"
		}
	}
	s.log(fmt.Sprintf("活动：%v  赛季：%v  累计积分：%v",
		orDash(data["activity_name"]), orDash(data["season"]), orDash(data["total_credits"])))
	s.log(fmt.Sprintf("今日已签：%v  连续天数：%v  本周：%s",
		orDash(data["today_checked_in"]), orDash(data["streak_days"]), dots))
	return 0, data
}

// Refresh 仅续期令牌。
func (s *Store) Refresh() int {
	c := s.loadStore()
	if c == nil {
		c = s.bootstrapFromDesktop()
		if c == nil {
			s.log("无凭据可续期，请先跑一次 claim 引导。")
			return 2
		}
	}
	if s.DoRefresh(c) {
		return 0
	}
	return 1
}

// Doctor 体检：凭据来源与令牌形态。
func (s *Store) Doctor() {
	fmt.Println("== 本脚本凭据仓库 ==")
	st := s.loadStore()
	if st != nil {
		fmt.Printf("  路径        : %s\n", s.authStore())
		fmt.Printf("  uid         : %s\n", st.UID)
		fmt.Printf("  domain      : %s\n", st.Domain)
		fmt.Printf("  accessToken : len=%d\n", len(st.AccessToken))
		fmt.Printf("  refreshToken: len=%d\n", len(st.RefreshToken))
		if st.ExpiresAt != 0 {
			left := (float64(st.ExpiresAt)/1000 - float64(time.Now().Unix())) / 86400
			fmt.Printf("  有效期至    : %s（剩余 %.1f 天）\n",
				time.Unix(st.ExpiresAt/1000, 0).UTC().Format("2006-01-02 15:04 UTC"), left)
		}
	} else {
		fmt.Println("  （空，尚未引导）")
	}

	fmt.Println("== 桌面端登录态（只读扫描）==")
	boot := s.bootstrapFromDesktop()
	if boot != nil {
		fmt.Printf("  可用明文源 : %s\n", boot.src)
		fmt.Printf("  uid/domain : %s / %s\n", boot.UID, boot.Domain)
	} else {
		fmt.Println("  未找到含明文 refreshToken 的 .info 文件")
	}
	for _, d := range s.infoDirs {
		entries, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".info") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(d, e.Name()))
			if err != nil {
				continue
			}
			var j map[string]any
			if json.Unmarshal(b, &j) != nil {
				continue
			}
			auth, _ := j["auth"].(map[string]any)
			shape := "无"
			if at, ok := auth["accessToken"].(string); ok {
				shape = fmt.Sprintf("明文 len=%d", len(at))
			} else if at, ok := auth["accessToken"].(map[string]any); ok {
				keys := make([]string, 0, len(at))
				for k := range at {
					keys = append(keys, k)
				}
				shape = fmt.Sprintf("加密 %v", keys)
			}
			name := e.Name()
			if len(name) > 50 {
				name = name[:50]
			}
			fmt.Printf("    %-50s accessToken=%s\n", name, shape)
		}
	}
}

// ── 小工具 ────────────────────────────────────────────────────────────────

// hardenPermissions Windows 下 POSIX 位无效，用 NTFS ACL 真正收紧；返回**实际生效**的描述。
func hardenPermissions(path string) string {
	user := os.Getenv("USERNAME")
	if user == "" {
		return "未收紧（取不到 USERNAME）"
	}
	// icacls 收紧失败不阻断（凭据已写出）；对齐 Python「收紧失败(%s)」返回描述
	cmd := exec.Command("icacls", path, "/inheritance:r", "/grant:r", user+":F")
	if err := cmd.Run(); err != nil {
		return "收紧失败(exec)"
	}
	return "NTFS ACL：仅 " + user
}

func asStr(v any) string {
	s, _ := v.(string)
	return s
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func orDash(v any) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%v", v)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func first8(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func marshalIndent2(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
