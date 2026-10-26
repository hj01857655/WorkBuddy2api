// Package dump 对应 Python 版 server.py 的请求/响应原文落盘：
// dumps/<MMDD-HHMMSS-pPID>.<rid>.<kind>.txt（超 512KB 截断加 .truncated 后缀），
// 超量按 mtime 删最旧。只落 body 原文，绝不落 HTTP header——Authorization
// 里是明文网关 key，且 body 需可直接重放。
package dump

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const maxBytes = 512 * 1024

// Writer 落盘器；On=false 时所有调用都是 no-op。
type Writer struct {
	On       bool
	Dir      string
	MaxFiles int // ≤0 不轮转

	mu      sync.Mutex
	bootTag string
}

func New(on bool, dir string, maxFiles int) *Writer {
	return &Writer{
		On:       on,
		Dir:      dir,
		MaxFiles: maxFiles,
		bootTag:  fmt.Sprintf("%s-p%d", time.Now().Format("0102-150405"), os.Getpid()),
	}
}

// Save 把请求/响应原文落盘到 <bootTag>.<rid>.<kind>.txt。
// rid 每进程从 r0001 重新计数，bootTag 里的启动时间+pid 防跳重启后同名 rid 覆盖旧证据。
func (w *Writer) Save(rid, kind, data string) {
	if !w.On || data == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := os.MkdirAll(w.Dir, 0o700); err != nil {
		slog.Warn("落盘失败", slog.String("rid", rid), slog.String("err", err.Error()))
		return
	}
	truncated := len(data) > maxBytes
	name := fmt.Sprintf("%s.%s.%s.txt", w.bootTag, rid, kind)
	if truncated {
		name += ".truncated"
	}
	path := filepath.Join(w.Dir, name)
	if len(data) > maxBytes {
		data = data[:maxBytes]
	}
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		slog.Warn("落盘失败", slog.String("rid", rid), slog.String("err", err.Error()))
		return
	}
	w.trim()
}

// trim dump 数量超过 MaxFiles 时按 mtime 删最旧（明文提示词会无限增长）。
func (w *Writer) trim() {
	if w.MaxFiles <= 0 {
		return
	}
	entries, err := os.ReadDir(w.Dir)
	if err != nil {
		return
	}
	type fileInfo struct {
		path  string
		mtime time.Time
	}
	files := make([]fileInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fileInfo{path: filepath.Join(w.Dir, e.Name()), mtime: info.ModTime()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mtime.Before(files[j].mtime) })
	// 未超量直接返回：Go 负切片下标是 panic（Python 负切片=空集），不能照搬
	if len(files) <= w.MaxFiles {
		return
	}
	for _, stale := range files[:len(files)-w.MaxFiles] {
		if err := os.Remove(stale.path); err == nil {
			slog.Debug("dump 轮转删除", slog.String("path", stale.path))
		}
	}
}
