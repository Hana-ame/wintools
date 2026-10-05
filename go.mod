module github.com/Hana-ame/wintools

go 1.26.7

require (
	github.com/Hana-ame/go-peerserver v0.2.0
	github.com/andybalholm/brotli v1.2.2
	github.com/coder/websocket v1.8.15
	github.com/gin-gonic/gin v1.12.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.3
	github.com/klauspost/compress v1.19.2
	github.com/pion/ice/v4 v4.2.7
	github.com/pion/webrtc/v4 v4.2.15
	github.com/refraction-networking/utls v1.8.2
	golang.org/x/net v0.58.0
	golang.org/x/time v0.14.0
)

require (
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic v1.15.0 // indirect
	github.com/bytedance/sonic/loader v0.5.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/gabriel-vasile/mimetype v1.4.12 // indirect
	github.com/gin-contrib/sse v1.1.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.30.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pion/datachannel v1.6.0 // indirect
	github.com/pion/dtls/v3 v3.1.4 // indirect
	github.com/pion/interceptor v0.1.45 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/mdns/v2 v2.1.0 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtcp v1.2.16 // indirect
	github.com/pion/rtp v1.10.2 // indirect
	github.com/pion/sctp v1.10.0 // indirect
	github.com/pion/sdp/v3 v3.0.18 // indirect
	github.com/pion/srtp/v3 v3.0.11 // indirect
	github.com/pion/stun/v3 v3.1.5 // indirect
	github.com/pion/transport/v4 v4.0.2 // indirect
	github.com/pion/turn/v5 v5.0.9 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.59.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.3.1 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	go.mongodb.org/mongo-driver/v2 v2.5.0 // indirect
	golang.org/x/arch v0.22.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

// 信令服务器只有一份代码：peerdrive 主仓 back/signalserver。
// 独立仓 github.com/Hana-ame/go-peerserver 曾是它的镜像，但已于
// 2026-10-05 archive（只读），故本仓改为直接 replace 到主仓目录，
// 与 peerdrive 自身 back/go.mod 的做法一致。
//
// replace 目标是**绝对路径的仓外目录**，故本仓在别的机器/别的
// checkout 位置克隆后需要改这一行（或删掉 replace 回到版本化依赖）；
// peerdrive 侧的版本以该目录的 git 提交为准。
//
// API 兼容：v0.2.0 与主仓目录都提供 NewServer / Option /
// WithTokenWhitelist（主仓另有新增的 WithCORSOrigins），故本仓代码
// 两种情况都能编译。
replace github.com/Hana-ame/go-peerserver => /home/lumin/Workplace/peerdrive/back/signalserver
