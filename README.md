# Rust AI Bridge

Rust AI Bridge 是一个面向 Windows Server 的轻量 OpenAI API 中转程序。它提供原生 Win32 图形界面和系统托盘，在本机监听 OpenAI 兼容的 `/v1/*` 请求，并将普通 HTTP 与 SSE 流式响应透明转发到 Sub2API 或 CLIProxyAPI。

## 功能

- 保存多个 Sub2API/CLIProxyAPI 上游，并手动选择当前上游。
- 使用独立中转 Key 对客户端鉴权，转发时替换成上游 API Key。
- 透明代理 `/v1/*` 的方法、路径、查询参数、请求体、状态码和响应体。
- 支持 Chat Completions、Responses 等接口的 SSE 流式响应。
- 对符合条件的 Sub2API Responses 流提供一次安全重试，并在等待期间发送 SSE 心跳。
- 支持通过 `/{effort}/responses` 为 Responses 请求设置思考等级。
- 上游切换时立即取消旧上游的活动请求。
- 原生 Windows 界面、系统托盘、请求指标和最近 500 条日志。
- 可临时开启敏感调试捕获，在界面查看提取参数、请求正文和上游响应正文。
- API Key 使用 Windows DPAPI 加密，配置存放在当前用户的 LocalAppData。

## 构建

需要 Windows x64、Rust stable MSVC 工具链和 Windows SDK：

```powershell
cargo test
cargo build --release
```

输出文件：

```text
target\release\rust-ai-bridge.exe
```

Release 构建使用 Windows GUI 子系统，不会显示控制台窗口。

## 使用

1. 打开程序，在“上游”页新增 Sub2API 或 CLIProxyAPI 的 Base URL 和 API Key。
2. 选择该配置并点击“设为当前”。
3. 在“设置”页确认监听地址和端口，复制自动生成的中转 Key。
4. 返回“总览”并点击“启动代理”。
5. 客户端使用总览页显示的 Base URL，并将中转 Key 作为 OpenAI API Key。

例如默认配置：

```text
Base URL: http://服务器地址:8317/v1
API Key:  rab_xxxxxxxxxxxxxxxxx
```

### 通过 URL 设置思考等级

专用路由格式为：

```text
POST http://服务器地址:8317/{effort}/responses
```

仅支持 `low`、`medium`、`high`、`xhigh`、`max`。例如客户端请求：

```text
POST http://服务器地址:8317/high/responses
```

程序会将它转发为上游的 `/v1/responses`，并在 JSON 请求体中强制设置：

```json
{
  "reasoning": {
    "effort": "high"
  }
}
```

URL 中的等级优先于请求正文原有的 `reasoning.effort`，查询参数会保留。所有发往 `/v1/responses` 的请求都会自动删除顶层 `max_output_tokens`，避免不兼容上游拒绝该参数；其他 `/v1/*` 路由仍保持透明转发。此功能只修改 Responses 请求参数，不会把 Chat Completions 的消息格式转换成 Responses 格式。

`low`、`medium`、`high`、`xhigh`、`max` 五档等级共用完全相同的处理链路，等级本身只决定写入 `reasoning.effort` 的值。字段过滤、匿名会话映射、`prompt_cache_key` 注入、CLIProxyAPI `Session-Id`、安全重试判断、鉴权替换、日志记录和 SSE 流式处理规则均保持一致。缓存行为仍取决于客户端是否提供稳定的会话标识；没有稳定会话标识时，不会仅因为使用了某个思考等级就强制启用缓存。

使用 OpenAI SDK 时，可以把 Base URL 设为 `http://服务器地址:8317/high`，再调用 Responses API。

### 多用户会话隔离

中转 Key 仅用于鉴权，不作为用户或会话标识。多个用户共用同一中转 Key 时，客户端可提供以下请求头：

- `X-RAB-User-Id`：稳定的用户命名空间，避免不同用户使用相同会话 ID 时冲突。
- `X-RAB-Session-Id`：稳定的会话 ID，优先级最高。
- `X-Prompt-Cache-Id`：兼容已有客户端，在没有 `X-RAB-Session-Id` 时自动作为会话 ID。

Responses 正文已有的 `prompt_cache_key` 也会参与会话映射。程序使用独立的安装密钥生成匿名标识，并将其写入 Responses 的 `prompt_cache_key`；CLIProxyAPI 同时收到相同的 `Session-Id`。`X-RAB-*` 原始值不会转发给上游。

客户端如需跨请求保持缓存与会话连续性，必须提供稳定会话标识。没有任何会话标识时，Sub2API 请求不会注入 `prompt_cache_key`；CLIProxyAPI 每个请求使用不同的一次性 `Session-Id`，优先保证不同用户之间不会错误合并。

### 调试捕获

在“日志”页勾选“捕获请求/响应正文（敏感）”后，新请求的详情可通过双击日志或“查看详情”打开：

- 显示客户端请求头和实际转发到上游的请求头；鉴权、Cookie、API Key、Token 等敏感头值会自动隐藏。
- 请求正文和响应正文小于等于 128 KiB 时尽可能完整保留；更大时保留首部和尾部各 64 KiB，并标明中间省略的字节数。
- 提取的常用参数最多保留 16 KiB。
- Responses 请求会额外显示“实际上游请求结构”，列出顶层字段、input 项的类型/角色/状态/ID/content 类型，以及 tools 类型；提示词、文本内容、工具描述和 JSON Schema 不会写入该结构摘要。
- 数据仅保存在当前进程内存中，不写入文件日志。
- 关闭开关会立即清除内存中的调试详情；程序重启后开关恢复为关闭。
- SSE 响应会显示原始事件流首尾和完整事件类型计数，并提取终止事件、Response ID、状态、错误及 incomplete 原因；delta 内容不会复制到事件摘要。
- 对 Sub2API 的流式 Responses 请求，如果上游只返回 `response.created`、`response.in_progress`、`response.queued` 或空 reasoning 前导事件便断流，Bridge 会丢弃这段未向客户端展示的前导流，并使用完全相同的规范化正文、上游鉴权和匿名会话信息安全重试一次。
- 一旦出现文本、拒答、函数参数、非空输出或未知事件，Bridge 会立即透传并永久禁止该请求重试，避免重复回答或重复副作用。`background`、`conversation`、`previous_response_id`、内置工具请求以及 CLIProxyAPI 请求也不启用该重试；自定义 `function` 工具仍可参与，因为重试只发生在函数调用参数出现之前。
- 安全重试前最多缓冲 2 MiB 前导事件；等待期间每 5 秒发送一次 `: rab-keepalive` SSE 注释心跳。超过缓冲限制后立即透传并禁用重试。
- `/v1/responses`（包括 `/{effort}/responses` 转发后的路径）的 SSE 在 HTTP 200 后如果没有 `response.completed`、`response.failed`、`response.incomplete`、`response.cancelled` 或 `error` 终止事件，会记录为“上游流提前结束”并计入失败；如果安全重试后仍缺少终止事件，日志会明确标记重试次数。Chat Completions 等其他 SSE 不要求 Responses 终止事件。
- API 不会返回模型隐藏的内部思维过程。

## 安全说明

- 程序自身只监听 HTTP，不内置 TLS，也不会自动修改 Windows 防火墙。
- 公网部署必须在前方使用 IIS、Caddy、Nginx 等提供 HTTPS，并限制可访问来源。
- 文件日志不会记录 API Key、请求正文或响应正文；敏感调试正文仅在显式开启后显示于内存界面。
- 本程序不支持 WebSocket/Realtime API、协议转换、自动故障转移、计费或多用户 Key。
