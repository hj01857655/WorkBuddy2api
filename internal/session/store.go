// Package session 保存 Responses API 的会话状态，实现 previous_response_id /
// conversation / store 三个字段的语义（上游没有会话存储端点，只能在网关侧做）。
//
// 模型：
//
//	store:true 的每个响应，按 response.id 存一份（input items + output items）；
//	客户端下次带 previous_response_id=<id> 时，取出那份历史前置到新 input 之前；
//	conversation=<id> 则以会话为单位累积（等价于把一串 response 串成一条链）。
//
// 落地在状态目录 sessions.json（0600，原子写，按 LRU 上限裁剪）。进程内缓存，
// 重启后从文件恢复。
package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record 一个已保存的响应。
type Record struct {
	ID           string         `json:"id"`
	Conversation string         `json:"conversation,omitempty"`
	Input        []any          `json:"input"`  // 请求侧 input items
	Output       []any          `json:"output"` // 响应侧 output items
	Model        string         `json:"model"`
	Created      int64          `json:"created"`
	Usage        map[string]any `json:"usage,omitempty"`
}

// Store 会话仓库。
type Store struct {
	mu     sync.Mutex
	path   string
	byID   map[string]*Record
	byConv map[string][]string // conversation -> 顺序的 response id
	max    int
}

const defaultMax = 500

// New 打开（或新建）会话仓库。path 为空则只在内存里存。
func New(path string) *Store {
	s := &Store{byID: map[string]*Record{}, byConv: map[string][]string{}, max: defaultMax, path: path}
	if path == "" {
		return s
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return s
	}
	var file struct {
		Records []*Record `json:"records"`
	}
	if json.Unmarshal(raw, &file) != nil {
		return s
	}
	for _, rec := range file.Records {
		if rec == nil || rec.ID == "" {
			continue
		}
		s.byID[rec.ID] = rec
		if rec.Conversation != "" {
			s.byConv[rec.Conversation] = append(s.byConv[rec.Conversation], rec.ID)
		}
	}
	return s
}

// Save 保存一个响应（store:true 时调用）。
func (s *Store) Save(rec *Record) {
	if s == nil || rec == nil || rec.ID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec.Created == 0 {
		rec.Created = time.Now().Unix()
	}
	s.byID[rec.ID] = rec
	if rec.Conversation != "" {
		ids := s.byConv[rec.Conversation]
		// 同一 id 不重复进链
		dup := false
		for _, id := range ids {
			if id == rec.ID {
				dup = true
				break
			}
		}
		if !dup {
			s.byConv[rec.Conversation] = append(ids, rec.ID)
		}
	}
	s.evictLocked()
	s.flushLocked()
}

// Chain 按 previous_response_id / conversation 取出历史 input items。
// 返回按时间正序的 items（直接可前置到新请求 input 之前）。
func (s *Store) Chain(prevID, conversation string) []any {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	if conversation != "" {
		ids = append(ids, s.byConv[conversation]...)
	}
	if prevID != "" {
		// 从 prevID 往前回溯整条链（prevID 的 input 里可能本身就继承过更早的）
		if rec := s.byID[prevID]; rec != nil {
			if rec.Conversation != "" && conversation == "" {
				ids = append(ids, s.byConv[rec.Conversation]...)
			}
			ids = append(ids, prevID)
		}
	}
	seen := map[string]bool{}
	out := []any{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		rec := s.byID[id]
		if rec == nil {
			continue
		}
		out = append(out, rec.Input...)
		out = append(out, rec.Output...)
	}
	return out
}

// evictLocked 超过上限时丢最旧的。
func (s *Store) evictLocked() {
	if s.max <= 0 || len(s.byID) <= s.max {
		return
	}
	// 按 Created 升序找最旧的若干条
	type kv struct {
		id string
		t  int64
	}
	all := make([]kv, 0, len(s.byID))
	for id, rec := range s.byID {
		all = append(all, kv{id, rec.Created})
	}
	for i := 1; i < len(all); i++ {
		for j := i; j > 0 && all[j].t < all[j-1].t; j-- {
			all[j], all[j-1] = all[j-1], all[j]
		}
	}
	drop := len(all) - s.max
	for i := 0; i < drop; i++ {
		rec := s.byID[all[i].id]
		delete(s.byID, all[i].id)
		if rec != nil && rec.Conversation != "" {
			ids := s.byConv[rec.Conversation]
			kept := ids[:0]
			for _, id := range ids {
				if id != all[i].id {
					kept = append(kept, id)
				}
			}
			if len(kept) == 0 {
				delete(s.byConv, rec.Conversation)
			} else {
				s.byConv[rec.Conversation] = kept
			}
		}
	}
}

// flushLocked 原子写盘（0600）。
func (s *Store) flushLocked() {
	if s.path == "" {
		return
	}
	records := make([]*Record, 0, len(s.byID))
	for _, rec := range s.byID {
		records = append(records, rec)
	}
	b, err := json.Marshal(struct {
		Records []*Record `json:"records"`
	}{records})
	if err != nil {
		return
	}
	if dir := filepath.Dir(s.path); dir != "" {
		_ = os.MkdirAll(dir, 0o700)
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		_ = os.Rename(tmp, s.path)
	}
}

// Size 当前保存的响应数（诊断用）。
func (s *Store) Size() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}
