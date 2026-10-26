// Package cred 对应 Python 版 lanes/credential.py：读取本机登录态 + 令牌续期
// （国内版 cn / 国际版 intl 双 realm）。
//
// 两个 realm 的凭证分槽存放，互不覆盖：
//
//	国内版 cn    ~/.workbuddy/checkin/auth.json        ← 历史路径，签到脚本维护的槽位
//	国际版 intl  ~/.workbuddy/checkin/auth-intl.json   ← oauth login --realm global 写入
//
// realm 决定三件事：请求 origin、UA、刷新端点。
// 令牌刷新结果只留在内存，不落盘——两个 auth 文件都归登录方（签到脚本 / oauth）所有。
package cred

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	CNOrigin     = "https://copilot.tencent.com"
	GlobalOrigin = "https://www.workbuddy.ai"
	// 刷新端点两 realm 同路径、只收 POST（国际版 GET 404、POST 401/12153——
	// 路由只收 POST，别把 GET 的 404 当成「端点不存在」）。
	RefreshPath = "/v2/plugin/auth/token/refresh"

	// 桌面端内置 CLI 子进程发的 UA：首段 WorkBuddy/<桌面版>，末段 CLI/<内置 CLI 版本>。
	// 上游按段校验（缺 "CLI/<版本>" 实测 400/code 12403），两 realm 段数不同、不通用。
	UACN   = "WorkBuddy/5.6.2 CLI/2.147.0"
	UAIntl = "WorkBuddy/5.8.4 workbuddy-ai/5.8.4 CLI/2.161.0"

	// 内存在令牌有效期：与 Python 版一致，25 分钟内复用，避免每请求都读盘/换票。
	tokenTTL = 25 * time.Minute
)

// 区域标识：domain / JWT issuer 里命中任意一个即判为该 realm。
// 国内版历史换过出口（copilot.tencent.com → codebuddy.cn → workbuddy.cn），三个都要认。
var cnMarkers = []string{"copilot.tencent.com", "codebuddy.cn", "workbuddy.cn"}
var intlMarkers = []string{"workbuddy.ai", "codebuddy.ai"}

var realmAliases = map[string]string{
	"cn": "cn", "china": "cn",
	"intl": "intl", "global": "intl", "international": "intl", "oversea": "intl",
}

// Realm 单个槽位的静态配置。
type Realm struct {
	Name      string
	Label     string
	APIOrigin string
	UA        string
	Store     string
}

// Store 管理双 realm 凭据与内存令牌缓存；所有方法并发安全。
type Store struct {
	mu     sync.Mutex
	realms map[string]*Realm // name -> config（store 路径可被测试重定向）
	cache  map[string]*tokenEntry
	haveFn func(path string) bool // 覆盖"槽位是否有凭据"（测试注入用）
}

type tokenEntry struct {
	token string
	at    time.Time
}

// authFile 凭据文件结构：字段名与 Python 版完全一致（同一份文件两边读写）。
type authFile struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    int64  `json:"expiresAt"`
	UID          string `json:"uid"`
	Nickname     string `json:"nickname"`
	Domain       string `json:"domain"`
	EnterpriseID string `json:"enterpriseId"`
	RealmMark    string `json:"_realm"`
}

// NewDefaultStore 建标准槽位（~/.workbuddy/checkin/）。
func NewDefaultStore() *Store {
	return NewStore(defaultStoreDir())
}

func defaultStoreDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ".workbuddy", "checkin")
}

// NewStore 用指定目录建双槽位（测试/自定义部署用）。
func NewStore(dir string) *Store {
	return &Store{
		realms: map[string]*Realm{
			"cn":   {Name: "cn", Label: "国内版", APIOrigin: CNOrigin, UA: UACN, Store: filepath.Join(dir, "auth.json")},
			"intl": {Name: "intl", Label: "国际版", APIOrigin: GlobalOrigin, UA: UAIntl, Store: filepath.Join(dir, "auth-intl.json")},
		},
		cache: map[string]*tokenEntry{},
	}
}

// SetStorePath 重定向某 realm 的凭据文件（测试用；对应 Python 测试里改 REALMS）。
func (s *Store) SetStorePath(realm, path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.realms[NormalizeRealm(realm)]; ok {
		r.Store = path
	}
}

// ResetCache 清空内存令牌缓存（测试隔离用）。
func (s *Store) ResetCache() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = map[string]*tokenEntry{}
}

// NormalizeRealm 把 global/cn/intl 之类写法统一成槽位名；不认识返回空串。
func NormalizeRealm(name string) string {
	return realmAliases[strings.ToLower(strings.TrimSpace(name))]
}

// ConfiguredRealm 环境变量 WB_REALM（未设/写错返回空串）。
func ConfiguredRealm() string { return NormalizeRealm(os.Getenv("WB_REALM")) }

// StorePath 返回该 realm 的凭据文件路径（未知名回落 cn）。
func (s *Store) StorePath(realm string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.realm(realm).Store
}

func (s *Store) realm(realm string) *Realm {
	name := NormalizeRealm(realm)
	if name == "" {
		name = "cn" // DEFAULT_REALM
	}
	return s.realms[name]
}

// JWTIssuer 从 JWT 中段取 issuer（不验签，只读 claims）；不是 JWT 就返回空串。
func JWTIssuer(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return ""
	}
	seg := parts[1]
	if pad := len(seg) % 4; pad != 0 {
		seg += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(seg)
	if err != nil {
		return ""
	}
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil {
		return ""
	}
	iss, _ := payload["iss"].(string)
	return iss
}

// RealmOf 判定一份凭据属于哪个 realm：显式标记 > 域名 > 令牌 issuer > 默认 cn。
func RealmOf(data map[string]any) string {
	if marked := NormalizeRealm(asStr(data["_realm"])); marked != "" {
		return marked
	}
	haystacks := []string{strings.ToLower(asStr(data["domain"]))}
	if token := asStr(data["accessToken"]); token != "" {
		haystacks = append(haystacks, strings.ToLower(JWTIssuer(token)))
	}
	for _, text := range haystacks {
		if containsAny(text, intlMarkers) {
			return "intl"
		}
		if containsAny(text, cnMarkers) {
			return "cn"
		}
	}
	return "cn"
}

func containsAny(s string, markers []string) bool {
	for _, m := range markers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

func asStr(v any) string {
	s, _ := v.(string)
	return s
}

// readMap 读凭据文件为 map（保留原始字段，realmOf 判定需要任意键）。
func readMap(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("凭据文件无法读取：%w", err)
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("凭据文件无法解析：%w", err)
	}
	return data, nil
}

// hasCredentials 槽位里是否可用（accessToken + uid 齐）。
func (s *Store) hasCredentials(path string) bool {
	if s.haveFn != nil {
		return s.haveFn(path)
	}
	data, err := readMap(path)
	if err != nil {
		return false
	}
	return asStr(data["accessToken"]) != "" && asStr(data["uid"]) != ""
}

// AvailableRealms 哪些槽位里有可用凭据（顺序固定 cn → intl）。
func (s *Store) AvailableRealms() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, name := range []string{"cn", "intl"} {
		if s.hasCredentials(s.realms[name].Store) {
			out = append(out, name)
		}
	}
	return out
}

// Resolve 决定本次请求用哪个 realm：显式 > WB_REALM > 有凭据的槽位（cn 优先）。
func (s *Store) Resolve(realm string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resolveLocked(realm)
}

func (s *Store) resolveLocked(realm string) (string, error) {
	if wanted := NormalizeRealm(realm); wanted != "" {
		if !s.hasCredentials(s.realms[wanted].Store) {
			r := s.realms[wanted]
			return "", fmt.Errorf(
				"%s 尚无凭据（%s）；先登录：workbuddy2api login --realm %s",
				r.Label, r.Store, loginArg(wanted))
		}
		return wanted, nil
	}
	if env := ConfiguredRealm(); env != "" {
		return s.resolveLocked(env)
	}
	var have []string
	for _, name := range []string{"cn", "intl"} {
		if s.hasCredentials(s.realms[name].Store) {
			have = append(have, name)
		}
	}
	if len(have) == 0 {
		return "", fmt.Errorf("未找到任何凭据：%s（国内版）或 %s（国际版）；请先登录：workbuddy2api login",
			s.realms["cn"].Store, s.realms["intl"].Store)
	}
	return have[0], nil
}

func loginArg(realm string) string {
	if realm == "intl" {
		return "global"
	}
	return "cn"
}

// readFile 读并校验该 realm 槽位的凭据（resolve 后的槽位名传入）。
func (s *Store) readFile(name string) (map[string]any, error) {
	path := s.realm(name).Store
	data, err := readMap(path)
	if err != nil {
		return nil, err
	}
	if asStr(data["_realm"]) == "" {
		data["_realm"] = name // 旧文件没标记，按判定结果补上，后续分支少一次判断
	}
	for _, key := range []string{"accessToken", "uid"} {
		if asStr(data[key]) == "" {
			return nil, fmt.Errorf("凭据缺少字段 %s，请重新引导", key)
		}
	}
	return data, nil
}

// APIOrigin 聊天/模型网关地址；source 传凭据 map 或 realm 名。
func (s *Store) APIOrigin(source any) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if data, ok := source.(map[string]any); ok {
		return s.realms[RealmOf(data)].APIOrigin
	}
	name, err := s.resolveLocked(asStr(source))
	if err != nil {
		return s.realm(asStr(source)).APIOrigin
	}
	return s.realms[name].APIOrigin
}

// UserAgent 该 realm 的桌面端 UA（上游按段校验，两 realm 不通用）。
func (s *Store) UserAgent(realm string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name := NormalizeRealm(realm); name != "" {
		return s.realms[name].UA
	}
	name, err := s.resolveLocked("")
	if err != nil {
		return s.realms["cn"].UA
	}
	return s.realms[name].UA
}

// RefreshOrigin 刷新端点与账号同域（CN 实测在 www.workbuddy.cn）；域名为空时回落 realm 默认值。
func (s *Store) RefreshOrigin(data map[string]any) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if domain := strings.TrimSpace(asStr(data["domain"])); domain != "" {
		return "https://" + domain
	}
	return s.realms[RealmOf(data)].APIOrigin
}

// Token 取当前可用 Bearer；命中该 realm 的内存缓存直接返回。
func (s *Store) Token(realm string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, err := s.resolveLocked(realm)
	if err != nil {
		return "", err
	}
	if e, ok := s.cache[name]; ok && e.token != "" && time.Since(e.at) < tokenTTL {
		return e.token, nil
	}
	data, err := s.readFile(name)
	if err != nil {
		return "", err
	}
	tok := asStr(data["accessToken"])
	s.cache[name] = &tokenEntry{token: tok, at: time.Now()}
	return tok, nil
}

// Refresh 用旧 Bearer 换新令牌（401 自愈用）；只更新内存缓存，不落盘——
// auth 文件归登录方所有。
func (s *Store) Refresh(realm string) (string, error) {
	s.mu.Lock()
	name, err := s.resolveLocked(realm)
	if err != nil {
		s.mu.Unlock()
		return "", err
	}
	s.mu.Unlock()

	old, err := s.Token(name)
	if err != nil {
		return "", err
	}
	data, err := readMap(s.StorePath(name))
	if err != nil {
		return "", err
	}
	url := s.RefreshOrigin(data) + RefreshPath
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(""))
	if err != nil {
		return "", fmt.Errorf("令牌刷新请求构造失败：%w", err)
	}
	req.Header.Set("Authorization", "Bearer "+old)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.UserAgent(name))
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("令牌刷新请求失败：%w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("令牌刷新失败 HTTP %d: %s", resp.StatusCode, trunc(string(raw), 200))
	}
	var body map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			return "", fmt.Errorf("令牌刷新返回非 JSON：%s", trunc(string(raw), 200))
		}
	}
	payload, _ := body["data"].(map[string]any)
	newToken := firstNonEmpty(asStr(payload["accessToken"]), asStr(payload["access_token"]))
	if newToken == "" {
		return "", fmt.Errorf("令牌刷新响应未包含新令牌：%s", trunc(string(raw), 200))
	}
	s.mu.Lock()
	s.cache[name] = &tokenEntry{token: newToken, at: time.Now()}
	s.mu.Unlock()
	return newToken, nil
}

// Account 账号快照，对应 Python 版 load()。
type Account struct {
	Realm      string `json:"realm"`
	RealmLabel string `json:"realm_label"`
	UID        string `json:"uid"`
	Nickname   string `json:"nickname"`
	Domain     string `json:"domain"`
	Token      string `json:"-"`
	ExpiresAt  int64  `json:"expires_at"`
}

// Load 返回当前账号与令牌快照。
func (s *Store) Load(realm string) (Account, error) {
	s.mu.Lock()
	name, err := s.resolveLocked(realm)
	if err != nil {
		s.mu.Unlock()
		return Account{}, err
	}
	r := s.realms[name]
	s.mu.Unlock()

	data, err := s.readFile(name)
	if err != nil {
		return Account{}, err
	}
	tok, err := s.Token(name)
	if err != nil {
		return Account{}, err
	}
	exp, _ := data["expiresAt"].(float64)
	return Account{
		Realm: name, RealmLabel: r.Label,
		UID: asStr(data["uid"]), Nickname: asStr(data["nickname"]),
		Domain: asStr(data["domain"]), Token: tok, ExpiresAt: int64(exp),
	}, nil
}

// RealmStatus 面板用的单槽位状态行（json 字段与 Python status() 一致）。
type RealmStatus struct {
	Realm      string `json:"realm"`
	Label      string `json:"label"`
	Store      string `json:"store"`
	APIOrigin  string `json:"api_origin"`
	Present    bool   `json:"present"`
	UID        string `json:"uid"`
	Nickname   string `json:"nickname"`
	Domain     string `json:"domain"`
	ExpiresAt  int64  `json:"expires_at"`
	RemainText string `json:"remain_text"`
}

// Status 两个槽位的状态（面板用）：无凭据的槽位也列出来，带提示。
func (s *Store) Status() []RealmStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := []string{"cn", "intl"} // 顺序固定 cn → intl（Python 版同）
	rows := make([]RealmStatus, 0, 2)
	for _, name := range names {
		r := s.realms[name]
		row := RealmStatus{Realm: name, Label: r.Label, Store: r.Store,
			APIOrigin: r.APIOrigin, Present: s.hasCredentials(r.Store), RemainText: "—"}
		if row.Present {
			if data, err := readMap(r.Store); err == nil {
				row.UID = asStr(data["uid"])
				row.Nickname = asStr(data["nickname"])
				row.Domain = asStr(data["domain"])
				exp, _ := data["expiresAt"].(float64)
				row.ExpiresAt = int64(exp)
				row.RemainText = RemainText(row.ExpiresAt)
			} else {
				row.Present = false
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// RemainText 把毫秒时间戳转成人类可读的剩余有效期（与 Python remain_text 一致）。
func RemainText(expiresAt int64) string {
	remain := float64(expiresAt)/1000.0 - float64(time.Now().Unix())
	if remain <= 0 {
		return "已过期（请求时自动刷新）"
	}
	days := int64(remain) / 86400
	hours := (int64(remain) % 86400) / 3600
	if days > 0 {
		return fmt.Sprintf("%d 天 %d 小时", days, hours)
	}
	return fmt.Sprintf("%d 小时", hours)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
