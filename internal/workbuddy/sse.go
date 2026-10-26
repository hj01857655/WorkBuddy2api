// Package workbuddy 的 SSE 侧：逐帧解析上游 Chat SSE，产出 (kind, payload)。
//
// kind ∈ {content, reasoning, tool_calls, finish, usage, done}。
package workbuddy

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"sort"
	"strings"
)

// ErrStreamInterrupted 上游流被中断（读错误），调用方据此记 stream 中断日志。
var ErrStreamInterrupted = errors.New("upstream stream ended prematurely")

// SSEEvent 一帧解析结果，kind ∈ {content, reasoning, tool_calls, finish, usage, done}。
type SSEEvent struct {
	Kind  string
	Value any
}

// SSESeq IterSSE 的 iter 形态：产出结构化事件流；消费完后调用 errFn 得到
// 底层读错误（nil = 正常 EOF），调用方据此区分上游断流。
func SSESeq(body io.Reader) (seq iter.Seq[SSEEvent], errFn func() error) {
	var readErr error
	errFn = func() error { return readErr }
	seq = func(yield func(SSEEvent) bool) {
		readErr = IterSSE(body, func(kind string, value any) bool {
			return yield(SSEEvent{Kind: kind, Value: value})
		})
	}
	return seq, errFn
}

// IterSSE 严格按官方 parseSSELine 的语义：空行派发、":" 开头是心跳注释、
// 只剥一个前导空格。坏帧跳过，不中断整条流。
// emit 返回 false 表示调用方要提前终止（读到首帧即断开的探测走这条路）。
func IterSSE(body io.Reader, emit func(kind string, value any) bool) error {
	sc := bufio.NewScanner(body)
	// 单帧 JSON 可能很大（工具参数、长思维链），上限放到 16MB；
	// Python 版 iter_lines 没有长度上限，这里必须显式给，否则超长行直接报错断流。
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var buf string
	stop := false

	for sc.Scan() && !stop {
		// Scanner 已按 \n 切行并去掉行尾换行符；这里再剥 \r（CRLF 上游）
		line := strings.TrimRight(sc.Text(), "\r")

		if line == "" {
			if buf == "" {
				continue
			}
			payload := buf
			buf = ""
			if strings.TrimSpace(payload) == "[DONE]" {
				emit("done", nil)
				return nil
			}
			var chunk map[string]any
			if json.Unmarshal([]byte(payload), &chunk) != nil || chunk == nil {
				continue
			}
			chunk = UnwrapDataLayer(chunk)
			for _, ci := range asList(chunk["choices"]) {
				choice, ok := ci.(map[string]any)
				if !ok {
					continue
				}
				delta := asMap(choice["delta"])
				// reasoning_content 优先；同帧两者都有时不要重复累加
				if rc := str(delta["reasoning_content"]); rc != "" {
					if !emit("reasoning", rc) {
						stop = true
						break
					}
				} else if r := str(delta["reasoning"]); r != "" {
					if !emit("reasoning", r) {
						stop = true
						break
					}
				}
				if ct := str(delta["content"]); ct != "" {
					if !emit("content", ct) {
						stop = true
						break
					}
				}
				if tcs := asList(delta["tool_calls"]); len(tcs) > 0 {
					if !emit("tool_calls", tcs) {
						stop = true
						break
					}
				}
				// 进行中是 ""（空串）而非 null，按值判，假值直接跳过
				if fr := str(choice["finish_reason"]); fr != "" {
					if !emit("finish", fr) {
						stop = true
						break
					}
				}
			}
			if !stop {
				if u := asMap(chunk["usage"]); len(u) > 0 {
					if !emit("usage", u) {
						stop = true
					}
				}
			}
			continue
		}

		if strings.HasPrefix(line, ":") {
			continue // 心跳注释
		}
		idx := strings.Index(line, ":")
		if idx == -1 {
			continue
		}
		field, value := line[:idx], line[idx+1:]
		value = strings.TrimPrefix(value, " ") // 只剥一个前导空格
		if field == "data" {
			buf += value
		}
	}
	if err := sc.Err(); err != nil {
		return errors.Join(ErrStreamInterrupted, err)
	}
	return nil
}

// UnwrapDataLayer 上游间歇性把帧多包一层 {"data":{...}}（Python 版直接读
// chunk["choices"] 会静默丢内容，是已知缺陷）。内层确实像 chat completion
// （有 choices 或 usage 或 object）才解包，避免误伤正常响应。
func UnwrapDataLayer(chunk map[string]any) map[string]any {
	if _, has := chunk["data"]; !has {
		return chunk
	}
	// 已经有顶层形状就不要解包（避免 data 是别的业务字段时被误当包装层）
	if _, ok := chunk["choices"]; ok {
		return chunk
	}
	if _, ok := chunk["usage"]; ok {
		return chunk
	}
	inner, ok := chunk["data"].(map[string]any)
	if !ok {
		return chunk
	}
	if _, ok := inner["choices"]; !ok {
		if _, ok := inner["usage"]; !ok {
			if _, ok := inner["object"]; !ok {
				return chunk
			}
		}
	}
	return inner
}

// toolSlot 非流式聚合时的 tool_call 累积槽。
type toolSlot struct {
	id        any
	name      string
	arguments strings.Builder
}

func (t *toolSlot) toMap() map[string]any {
	return map[string]any{
		"id":   t.id,
		"type": "function",
		"function": map[string]any{
			"name":      t.name,
			"arguments": t.arguments.String(),
		},
	}
}

// mergeToolCall 按 index 合并 tool_calls 增量。
//
// id 与 function.name 只在首帧有值（用赋值而非拼接，避免被空串覆盖）；
// arguments 是流式 JSON 片段，必须拼接后再解析。
func mergeToolCall(slots map[int]*toolSlot, call map[string]any) {
	index := 0
	if f, ok := call["index"].(float64); ok {
		index = int(f)
	}
	slot, ok := slots[index]
	if !ok {
		slot = &toolSlot{id: nil}
		slots[index] = slot
	}
	if id := call["id"]; id != nil && id != "" {
		slot.id = id
	}
	fn := asMap(call["function"])
	if name := str(fn["name"]); name != "" {
		slot.name = name
	}
	if args := str(fn["arguments"]); args != "" {
		slot.arguments.WriteString(args)
	}
}

func sortedCalls(slots map[int]*toolSlot) []any {
	if len(slots) == 0 {
		return nil
	}
	idxs := make([]int, 0, len(slots))
	for i := range slots {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	out := make([]any, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, slots[i].toMap())
	}
	return out
}
