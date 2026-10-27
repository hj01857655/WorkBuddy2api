// WorkBuddy2api-go 入口：对应 Python 版 server.py 的 __main__ 段。
//
// serve（默认）：装配配置/日志/凭据/上游客户端/落盘，打印凭证横幅，
// 起每日签到 daemon（启动补签 + 每天 0 点定点），监听 HTTP。
//
// 子命令（Python 版经由 lanes/checkin.py、lanes/oauth.py CLI，Go 版直接调库）：
//
//	workbuddy2api login [--realm cn|global]   OAuth 设备流登录并落盘凭据
//	workbuddy2api claim                       每日签到（幂等）
//	workbuddy2api checkin-status              查询签到状态
//	workbuddy2api refresh                     仅续期签到凭据
//	workbuddy2api doctor                      凭据体检（来源与令牌形态）
package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
	"workbuddy2api/internal/session"

	"workbuddy2api/internal/checkin"
	"workbuddy2api/internal/config"
	"workbuddy2api/internal/cred"
	"workbuddy2api/internal/dump"
	"workbuddy2api/internal/handler"
	"workbuddy2api/internal/workbuddy"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 {
		switch args[0] {
		case "login":
			runLogin(args[1:])
			return
		case "claim":
			os.Exit(checkin.NewStore().Claim())
			return
		case "checkin-status":
			code, _ := checkin.NewStore().Status(false)
			os.Exit(code)
			return
		case "refresh":
			os.Exit(checkin.NewStore().Refresh())
			return
		case "doctor":
			checkin.NewStore().Doctor()
			return
		case "serve", "run":
			args = nil // 显式 serve
		default:
			fmt.Fprintf(os.Stderr, "未知命令 %q；可用：login / claim / checkin-status / refresh / doctor，不带参数即启动服务\n", args[0])
			os.Exit(2)
		}
	}
	serve()
}

// runLogin OAuth 设备流登录：--realm cn（默认）/ global（国际版）。
func runLogin(args []string) {
	realm := "cn"
	for i := 0; i < len(args); i++ {
		if args[i] == "--realm" && i+1 < len(args) {
			realm = args[i+1]
			i++
		}
	}
	store := cred.NewDefaultStore()
	if _, err := store.Login(realm); err != nil {
		fmt.Fprintf(os.Stderr, "[oauth] 登录失败：%v\n", err)
		os.Exit(1)
	}
}

func serve() {
	cfg := config.Load()
	logger := handler.NewLogger(cfg.StateDir)
	dw := dump.New(cfg.DumpOn, cfg.DumpDir, cfg.DumpMaxFiles)

	store := cred.NewDefaultStore()
	wb := workbuddy.NewClient(store, cfg.StateDir)
	sessions := session.New(filepath.Join(cfg.StateDir, "sessions.json"))

	// 凭证横幅：两个槽位都列出来，缺哪个一眼可见（对齐 Python __main__）
	for _, row := range store.Status() {
		if row.Present {
			nickname := row.Nickname
			if nickname == "" {
				nickname = "未命名"
			}
			fmt.Printf("凭证账号 [%s] %s (%s)  剩余 %s\n",
				row.Label, nickname, row.UID, row.RemainText)
		} else {
			hint := "workbuddy2api doctor"
			if row.Realm == "intl" {
				hint = "workbuddy2api login --realm global"
			}
			fmt.Printf("凭证账号 [%s] 无（登录：%s）\n", row.Label, hint)
		}
	}
	// 默认 realm：与请求时的解析规则一致（显式 > WB_REALM > 有凭据的槽位）
	if account, err := store.Load(""); err == nil {
		fmt.Printf("默认 realm: %s\n", account.Realm)
	} else {
		fmt.Printf("凭证不可用: %v\n", err)
	}
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	fmt.Printf("WorkBuddy2api 已启动: http://%s\n", addr)
	fmt.Printf("OpenAI Base URL: http://%s/v1\n", addr)
	if cfg.APIKey != "" {
		fmt.Println("本地鉴权: 已开启（API_KEY）")
	} else {
		fmt.Println("本地鉴权: 未开启（仅回环）")
	}

	go checkinLoop(logger)

	srv := &handler.Server{Cfg: cfg, Cred: store, WB: wb, Dump: dw, Log: logger, Sessions: sessions}
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		logger.Error("HTTP 服务退出", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

// checkinLoop 每日自动签到（qoder2api 同款：daemon 线程，启动补签 + 每天定点）。
// 签到幂等：已签返回「今天已签到过」，补签无副作用。日志走 slog（签到库的
// LogFn 桥接到 server 日志），与 Python daemon 的 logger.info 形态一致。
func checkinLoop(logger *slog.Logger) {
	st := checkin.NewStore()
	st.LogFn = func(msg string) { logger.Info("[checkin] " + msg) }

	logger.Info("[checkin] 启动补签", slog.Int("exit", st.Claim()))
	for {
		now := time.Now()
		target := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		if !now.Before(target) {
			target = target.AddDate(0, 0, 1)
		}
		wait := target.Sub(now)
		logger.Info("[checkin] 下次签到",
			slog.String("at", target.Format("2006-01-02 15:04")),
			slog.Float64("waitS", wait.Seconds()))
		time.Sleep(wait)
		logger.Info("[checkin] 定时签到", slog.Int("exit", st.Claim()))
	}
}
