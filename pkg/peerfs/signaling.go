// Package peerfs 让浏览器通过标准 peerjs 库 (WebRTC DataChannel) 直连本地
// Go 节点，加载图片/视频等文件。
//
// 组成：
//
//   - Node: 包装 pkg/peerjs，在 DataChannel 上提供 list/read 文件服务（见 node.go）
//   - MountSignaling: 把自托管 PeerJS 信令挂到任意 http.ServeMux —— 可选开启，
//     不挂载则节点/浏览器使用公共云信令或其他 peerserver
//   - web/: 内嵌浏览器端页面（bridge.js 封装 + 文件浏览控制台），由节点 HTTP 服务
//
// 信令直接复用 peerdrive 拆出的独立模块 github.com/Hana-ame/go-peerserver
// （back/signalserver，peerjs-server 协议子集：注册/OFFER/ANSWER/CANDIDATE/
// LEAVE 按 dst 转发、离线队列、心跳、token 白名单）。
package peerfs

import (
	"net/http"

	signalserver "github.com/Hana-ame/go-peerserver"
)

// MountSignaling 把自托管 PeerJS 信令服务器挂载到 mux 上并启动后台清理循环，
// 返回 Server 以便复用（测试/扩展发现端点 HandleAnnounce/HandleNodes）。
//
// 挂载路径与 stock peerjs 客户端默认配置（path:"/"，key 作参数）对齐：
//
//	WS  /peerjs      信令消息（OFFER/ANSWER/CANDIDATE 按 dst 转发）
//	GET /peerjs/id   借 ID（随机分配）
//
// tokens 非空时启用信令 token 白名单：WS 连接的 token 必须在名单内才能升级，
// 防止任意客户端注册任意 ID 冒充节点收信令；空名单 = 不限制。
func MountSignaling(mux *http.ServeMux, key string, tokens []string) *signalserver.Server {
	srv := signalserver.NewServer(key, signalserver.WithTokenWhitelist(tokens))
	srv.Start()
	mux.HandleFunc("/peerjs", srv.HandleWS)
	mux.HandleFunc("/peerjs/id", srv.HandleID)
	return srv
}
