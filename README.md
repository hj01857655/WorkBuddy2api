# WorkBuddy2api (Go)

![Go](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white) ![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux-lightgrey) ![Type](https://img.shields.io/badge/type-API%20Gateway-blue)

WorkBuddy 转 OpenAI / Anthropic 兼容 API 的 Go 网关实现。

## 构建

```bash
go build -o workbuddy2api .
```

## 运行

| 环境变量 | 说明 |
|---|---|
| `API_KEY` | 客户端鉴权密钥 |
| `HOST` / `PORT` | 监听地址与端口 |
| `WB2API_STATE_DIR` | 状态目录 |
| `DUMP` / `DUMP_DIR` / `DUMP_MAX_FILES` | 请求转储开关与目录 |

## 接口

- `GET /health` — 健康检查
- `GET /admin` — Web 管理面板（内置，开箱即用）：凭据状态 / 双槽概览、模型目录与可调性、令牌刷新、模型探测。状态目录放一份自定义 `panel.html` 可覆盖内置版
- `GET /v1/models` — 模型列表
- `POST /v1/chat/completions` — OpenAI Chat 兼容
- `POST /v1/messages` — Anthropic Messages 兼容
- `POST /v1/responses` — OpenAI Responses 兼容

## 协议适配

三个入口都是 Chat Completions 的翻译层，请求 → Chat → 响应回译。下表是按官方字段
逐项核对后的实测状态（模型 `deepseek-v4.1-flash`）。

### 能力矩阵

| 能力 | `/v1/chat/completions` | `/v1/responses` | `/v1/messages` |
|---|:---:|:---:|:---:|
| 非流式 | ✅ | ✅ | ✅ |
| 流式 | ✅ | ✅ | ✅ |
| 工具调用（function / tool_use） | ✅ | ✅ | ✅ |
| 多轮工具结果回传 | ✅ | ✅ | ✅ |
| 并行工具调用 | ✅ | ✅ | ✅ |
| 图片输入 | ✅ | ✅ | ✅ |
| 思考内容（reasoning） | ✅ | ✅ | ✅ |
| 结构化输出 | — | ✅ `text.format` | — |
| 会话续接 | — | ✅ `previous_response_id` / `conversation` | — |
| 上下文截断 | — | ✅ `truncation` | — |
| 工具调用上限 | — | ✅ `max_tool_calls` | — |

### 流式事件序列（实测）

`/v1/messages`：`message_start` → `content_block_start` → `content_block_delta` →
`content_block_stop` → `message_delta` → `message_stop`

`/v1/responses`：`response.created` → `response.in_progress` → `response.output_item.added` →
`response.reasoning_summary_text.delta` → `response.output_text.delta` →
`response.reasoning_summary_text.done` → `response.output_item.done` →
`response.output_text.done` → `response.completed`

### 字段映射要点

- Anthropic `thinking: {type:"enabled", budget_tokens:N}` → 上游 `reasoning_effort`（N≥24000 high，≥8000 medium，否则 low）
- 上游 `reasoning_content` → Anthropic `thinking` block（`thinking` / `thinking_delta`）
- Responses `text.format`（json_schema）→ 压成 system 指令约束输出，实测可出合法 JSON
- Responses `reasoning.effort` → 上游 `reasoning_effort`
- Responses `truncation: {type:"auto", max_input_tokens:N}` → 网关按近似 token 上限裁掉最早的非 system 消息（1 token ≈ 3 字节）
- Responses `max_tool_calls: N` → 网关响应侧硬截断，超出即停并标 `incomplete_details.reason = "max_tool_calls"`
- Responses `previous_response_id` / `conversation` / `store` → 网关本地会话（`sessions.json`，0600 原子写，LRU 500 条）；`store:false` 不保存

### 显式忽略的字段

以下字段上游无对应能力，解析后直接丢弃（不影响请求成功）：

| 字段 | 原因 |
|---|---|
| `metadata` | 上游无响应存储与检索 |
| `include` | 上游不返回 `reasoning.encrypted_content` 等可选内容 |
| `service_tier` | 上游无计费档位 |
| `safety_identifier` | 上游风控基于账号令牌 |
| `background` | 需任务队列与轮询，不在协议翻译层 |
| `top_k`（Anthropic） | 上游无采样参数 |

## 双版本

同一进程同时支持国内版与国际版，按凭据槽位区分：

| | 国内版（cn） | 国际版（intl） |
|---|---|---|
| 登录 | `workbuddy2api login` | `workbuddy2api login --realm global` |
| 上游域名 | `www.workbuddy.cn` | `www.workbuddy.ai` |
| 凭据文件 | `~/.workbuddy/checkin/auth.json` | `~/.workbuddy/checkin/auth-intl.json` |

请求按 realm 路由，两槽位互不影响。

## Docker 部署

```bash
# 配置环境变量后启动
cp .env.example .env   # 按需填写
docker compose up -d
```

镜像同时发布到 GHCR：`ghcr.io/hj01857655/workbuddy2api`（跟随 v* 标签发布，含 latest，linux/amd64 + linux/arm64）。

二进制发版：推送 `v*` 标签后 Actions 自动构建 linux / windows / darwin 的 amd64 + arm64 六平台产物并发布 Release。
