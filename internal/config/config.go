// Package config 装配 WorkBuddy2api 的运行配置：.env 加载 + 监听/落盘/探测缓存路径。
//
// 与 Python 版 server.py 对齐的语义：
//
//	· HOST / PORT / API_KEY / WB_REALM / DUMP / DUMP_DIR / DUMP_MAX_FILES
//	· request.log、dumps/、models_probe*.json 落在状态目录
//	  （Python 版是脚本所在目录；Go 版是 exe 所在目录，可用 WB2API_STATE_DIR 覆盖，
//	  把 exe 放进原 WorkBuddy2api 目录即可无缝接管 Python 版的探测缓存）
//	· 可调性探测缓存按 realm 分文件：cn → lanes/models_probe.json（沿用 Python 版
//	  历史路径），其余 realm → lanes/models_probe_<realm>.json
package config

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	DefaultHost = "127.0.0.1"
	DefaultPort = 8377 // 避开 codearts2api 占用的 8787
)

type Config struct {
	APIKey       string
	Host         string
	Port         int
	StateDir     string // request.log / dumps/ / panel.html / lanes/models_probe*.json 所在
	DumpOn       bool
	DumpDir      string
	DumpMaxFiles int
	PanelPath    string
}

// StateFilePath 返回状态目录下的文件路径。
func (c *Config) StateFilePath(name string) string { return filepath.Join(c.StateDir, name) }

// Load 读取配置。优先级：环境变量 > .env > 默认值。
// .env 从状态目录和工作目录两处尝试（与 python-dotenv 的 load_dotenv 语义近似：
// 已有 env 不被 .env 覆盖）。
func Load() *Config {
	c := &Config{
		Host:         DefaultHost,
		Port:         DefaultPort,
		DumpOn:       true,
		DumpMaxFiles: 200,
	}

	// 状态目录：env 显式指定 > exe 所在目录
	c.StateDir = os.Getenv("WB2API_STATE_DIR")
	if c.StateDir == "" {
		if exe, err := os.Executable(); err == nil {
			c.StateDir = filepath.Dir(exe)
		} else {
			wd, _ := os.Getwd()
			c.StateDir = wd
		}
	}

	// .env：状态目录优先，其次工作目录；两处都有的键先到先得
	for _, dir := range []string{c.StateDir, mustWd()} {
		loadDotEnv(filepath.Join(dir, ".env"))
	}

	c.APIKey = strings.TrimSpace(os.Getenv("API_KEY"))
	if h := strings.TrimSpace(os.Getenv("HOST")); h != "" {
		c.Host = h
	}
	if p, err := strconv.Atoi(strings.TrimSpace(os.Getenv("PORT"))); err == nil && p > 0 {
		c.Port = p
	}
	if v := strings.TrimSpace(os.Getenv("DUMP")); v == "0" {
		c.DumpOn = false
	}
	if v := strings.TrimSpace(os.Getenv("DUMP_MAX_FILES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.DumpMaxFiles = n
		}
	}
	c.DumpDir = strings.TrimSpace(os.Getenv("DUMP_DIR"))
	if c.DumpDir == "" {
		c.DumpDir = filepath.Join(c.StateDir, "dumps")
	}
	c.PanelPath = filepath.Join(c.StateDir, "panel.html")
	return c
}

func mustWd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

// loadDotEnv 极简 .env 解析：KEY=VALUE 行，# 注释，不展开变量，不覆盖已有 env。
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if k != "" && os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
