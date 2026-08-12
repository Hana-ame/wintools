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
- **v2.0 孤儿分支重置计划(用户已确认,待执行)**:旧 main 最后一个 commit 打 tag `v1.9.0` + push(兜住旧历史);然后 `git checkout --orphan main` 把当前全部文件作为根 commit 建无历史新主线(时间戳伪造,时刻待定),打 tag `v2.0.0` + force push main + push tag(触发 release CI 发布 v2.0.0)。

## 项目结构
- `cmd/*` 为多个独立可执行程序(api-server / ech-proxy / local-proxy / local-proxy-detected / zen-proxy / zen-multi / localdns 等),CI 全部 build。
- `pkg/proxyheaders` — 请求/响应头透传工具。