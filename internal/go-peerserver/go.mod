// 本目录是 go-peerserver 信令服务器的**仓内副本**，由 peerdrive 主仓
// back/signalserver 同步而来（2026-10-05，peerdrive @ da36a29）。
//
// 为什么是副本而不是直接依赖：go-peerserver 独立仓已 archive（只读），
// 不能再作为发版与取码来源；而 peerdrive 主仓与本仓是两个独立仓，
// 跨仓取码在 Go module 里没有「直接引用兄弟仓目录」的机制——只能
// replace 到一个路径，而 peerdrive 的路径在本仓/CI 上并不存在。
// 故本仓带一份副本，并用 replace 指向它（见根 go.mod）。
//
// 同步口径：改动信令逻辑时，peerdrive 侧改完再同步到本目录（cp -r）。
// 方向是单向的 peerdrive → 本目录，不要在本目录单独改逻辑。
module github.com/Hana-ame/go-peerserver

go 1.26.2

require github.com/gorilla/websocket v1.5.3

require github.com/stretchr/testify v1.11.1

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
