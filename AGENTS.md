# wintools

## 关键约定

### Release 工作流(记住!)
- **推送 tag = 自动编译 + 发布 release**。`.github/workflows/release.yml` 监听 `v*` tag push,CI 全量 build 所有 `cmd/*` 并上传 GitHub release。
- 用户常跑在 **Windows**,改完代码必须**推 tag** 用户才能从 release 下载到对应 `.exe`。改代码后记得要 commit + 打新 tag + push。
- 版本号 bump:看 `gh release list` 当前最新版本,推下一个。例:v1.7.1 → v1.7.2。
- 推 tag: `git tag <vX.Y.Z> && git push origin main && git push origin <tag>`;用 `gh run watch <run_id>` 等 CI 通过,`gh release view <tag>` 确认资产。
- 资产命名:`<cmd>-<os>-<arch>.exe`,例 `local-proxy-windows-amd64.exe`。下载 `gh release download <tag> -p "<name>"`。
- Go CI 工作流 `go.yml` 也会在 push main 时跑,别忘 commit 干净工作区再推。
- **伪造时间戳约定(记住!)**:发布时 commit 和 tag 的时间戳要用 `GIT_AUTHOR_DATE` + `GIT_COMMITTER_DATE` 环境变量伪造(GitHub 显示 committer 时间,只设 `git commit --date` 没用)。具体伪造成什么时间,发布时由用户指定,默认取发布时刻前一天内。
- **v2.0 孤儿分支重置计划(用户已确认,待执行)**:旧 main 最后一个 commit 打 tag `v1.8.x`(发布含 zen 合并的最新版) + push(兜住旧历史);然后 `git checkout --orphan main` 把当前全部文件作为根 commit 建无历史新主线(时间戳伪造,时刻待定),打 tag `v2.0.0` + force push main + push tag(触发 release CI 发布 v2.0.0)。
- **v2.0.0 已完成: zen 家族并入 ech-proxy + ip-proxy**。旧 `cmd/zen-proxy`、`cmd/local-proxy-detected`、`cmd/zen-multi`、`cmd/capture-proxy`、`scripts/capture_proxy.go` 全部删除(v1.9.0 tag 存档)。v2.0.0 结构:
  - `cmd/ech-proxy`:zen.l.moonchan.xyz 走完整 provider 逻辑(`zen_provider.go`:auto 先 v6 再 v4 / v4 / v6,?stack= 指定栈,流检测/注入,3 次递进冷却,gzip 请求体,usage 计费)
  - `cmd/ip-proxy`:多 IP 分流 CONNECT 代理,`client`(JSON 配置 client.json 已 gitignore,六线 wss://vps|bwh|cloudcone-zen-v4|v6.moonchan.xyz,opencode 设 HTTPS_PROXY 选线)+ `server`(VPS 端 WS 隧道,`--force v4|v6`,纯 ws 由 nginx 反代,loopback 127.26.8.12/13:8080)

## 项目结构
- `cmd/*` 为多个独立可执行程序(ech-proxy / ip-proxy / kv-store / localdns / webrtc-proxy / opencode-proxy 等),CI 全部 build。
- `pkg/proxyheaders` — 请求/响应头透传工具。
- `pkg/netdial` — **Termux/Android 环境网络坑的公共修复**(重要):
  - Termux 无 `/etc/resolv.conf`,Go 纯解析器默认走 `[::1]:53` 会失败(`connection refused`),必须固定公共 DNS(8.8.8.8/1.1.1.1/223.5.5.5/114.114.114.114)。
  - Termux CA 在 `$PREFIX/etc/tls/cert.pem`,不在 Go 默认搜索路径,https 会报 `certificate signed by unknown authority`,需附加到 RootCAs。
  - 任何在 Termux 上跑、有出站请求的 proxy 一律用 `netdial.Client()/Dialer()/WebsocketDialOptions()`/`Transport()`,不要裸 `&http.Client{}` 或 `websocket.Dial(..., nil)`。已知坑过的:`opencode-proxy`(DNS+CA 都踩过,已修)、`ip-proxy`、`webrtc-proxy`、`peerjs`、`echproxy`、`apifwd`。ech-proxy 的 `zen_provider.go` 用 IP 直连 + InsecureSkipVerify,自带公共 DNS(resolveOnce),不受影响。
- `cmd/opencode-proxy`:aichat.moonchan.xyz 的 CORS chat proxy。`POST /chat/completion` → `https://opencode.ai/zen/go/v1/chat/completions`,CORS 仅放行 `https://aichat.moonchan.xyz`,key 从环境变量 `OPENCODE_GO_API_KEY` 读(不写死)。已部署 Termux `u0_a297.d.moonchan.xyz:8000`(ssh -p 8022,免代理直连,后台 `nohup`,PID 存 `~/opencode-proxy.pid`)。