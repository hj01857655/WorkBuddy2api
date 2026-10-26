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
- `GET /v1/models` — 模型列表
- `POST /v1/chat/completions` — OpenAI Chat 兼容
- `POST /v1/messages` — Anthropic Messages 兼容
- `POST /v1/responses` — OpenAI Responses 兼容
