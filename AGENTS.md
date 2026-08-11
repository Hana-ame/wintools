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
- **v2.0 计划新增: zen 家族已完成合并为 capture-proxy**。`cmd/zen-proxy` + `cmd/local-proxy-detected` + `cmd/zen-multi` + `scripts/capture_proxy.go`(+ cmd/local-proxy)合并为单个 `cmd/capture-proxy`,用 `-role` 区分两个角色:`provider`(zen-proxy + local-proxy-detected + capture_proxy 合体:Ollama 兼容端点,`--mode`/`/mode`=auto(先 v6 再 v4)/v4/v6,请求 ?stack=v4|v6 绕过 mode 直走指定栈,流检测/注入,/status/usage 统计,--detect=false=vanilla 透传,--ban 限流,可选 TLS);`multi`(zen-multi 聚合:转发到多台 provider,源列表暂时硬编码在 multi.go 顶部 defaultSources:cloudcone-v4/v6、bwh-v4/v6、vps-v4/v6,模型列表只列各源专属模型,无 auto/inf;不 failover:模型必须带源,只打该源,无源 400)。旧 cmd 已删除。发布后三台 VPS 的 systemd 服务(zen.service → capture-proxy.service)启动参数需换:原来的 zen-proxy positional args 改为 `capture-proxy -role provider ...`(无 argv 兼容,纯 flag)。

## 项目结构
- `cmd/*` 为多个独立可执行程序(capture-proxy / kv-store / ech-proxy / localdns / webrtc-proxy 等),CI 全部 build。
- `pkg/proxyheaders` — 请求/响应头透传工具。