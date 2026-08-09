# Changelog — v1.7.x

## v1.7.7 (2026-08-10)
- **zen-multi**: 转发给 zen-proxy 的请求体 gzip 压缩并带 `Content-Encoding: gzip`(压缩失败自动 fallback 明文)。
- **zen-proxy**: 收到 `Content-Encoding: gzip` 请求体时解压并去掉该头再转发;解压前后均有体积限制。
- **local-proxy-detected**: 请求级 httptrace 阶段日志(`dial/tls/wrote` 耗时 + proto);http2 响应头超时自动用 HTTP/1.1 重试一次(重建请求,修复复用耗尽 Body 导致 `ContentLength=X with Body length 0` 的 bug)。

## v1.7.6 (2026-08-09)
- **zen-proxy**: 新增第 7 位置参数 `mode`(`http`/`https`/空=自动),显式选择监听协议;`https` 时强制要求 cert/key,`http` 忽略 cert/key。

## v1.7.5 (2026-08-07)
- **local-proxy-detected**: 请求带 tools 且发生 stall / EOF / 硬断流 / 上游错误时,不再报 `UpstreamStall` 错,改注入 idle `echo 继续` tool_call(`finish_reason=tool_calls` + `[DONE]`),让客户端恢复工具循环。
- **local-proxy-detected**: 非工具流 (non-tool) 上游静默时给 **45s** think 宽限(thinkGrace),超过才判定 stall。
- **zen-multi**: 请求显式指定特定 endpoint(deepseek-v4-flash-* )时,失败**不再 fallback** 到其他 endpoint,直接返回错误。
- Release v1.7.5 的 GitHub Actions runner 曾拥堵失败,二进制已手动交叉编译部署。

## v1.7.4 (2026-08-07)
- **合并 vanilla / detected**:删除 `cmd/local-proxy`,vanilla + detected 合进单个 `local-proxy-detected`,用第 4 参数 `vanilla` 选择纯透传模式(detect 关闭)。
- **双栈监听**:同时监听 `0.0.0.0`(tcp4)与 `[::]`(tcp6)。
- **EOF [DONE] seal**:流截断但 EOF 时用 `[DONE]`/长度截断收尾,避免挂住。
- 文档更新 `docs/zen-proxy.md`。

## v1.7.3 (2026-08-07)
- **双栈监听**:`local-proxy` 同时监听 `0.0.0.0` 和 `[::]`。
- **usage 统计扩展**:`local-proxy` / `zen-multi` 的按家族(fam)/按上游(upstream)统计中加入 tokens / cache 数据。

## v1.7.2 (2026-08-06)
- **local-proxy-detected**:新增 per-model usage 统计(tokens / 计费 cost)与 `/stats` HTTP 端点。

## v1.7.1 (2026-08-06)
- **zen-multi**:任意 tools 请求发生在 upstream 错误事件时(只要 recoverable)都注入 idle 继续,不再仅限 inf。
- **SSE 健全化**:截断的 SSE 流按长度/`[DONE]` 收尾;stall 检测带心跳感知;`max_tokens` 放宽到 128k;lim-fail 走 cooldown 阈值。
- **local-proxy**:对齐 zen-proxy 的能力(stats + `/status` + stall guard)。
- 日志展示 `recoverable` 标志以便排查。

## v1.7.0 (2026-08-05)
- **从 Tools 迁入整套 Go zen proxy 栈**(`zen-proxy` / `zen-multi` / `local-proxy` 分立可执行程序),替代原 `zen_proxy.py`。
- **zen-proxy**:usage 统计、token 节奏(token-cadence)stall 检测、请求/响应头透传;`/status` 按 v4/v6 分列 usage。
- **zen-multi**:修复 injected 事件重复 `data: data:` 前缀;修复 `startReader` goroutine 泄漏(done channel)。
- **local-proxy**:公共 DNS 解析 + 以 IP 拨号并带 Host 头 + `InsecureSkipVerify`。
- **CI**:`CGO_ENABLED=0` + `-s -w` 静态构建(兼容老 glibc / Termux);release 矩阵后续调整为只保留 windows/amd64 等平台。

---

### 备注
- Release 由推 `v*` tag 触发 `.github/workflows/release.yml` 全量构建发布。v1.7.3 / v1.7.4 的 release 因 GitHub hosted runner 拥堵失败,可手动交叉编译。