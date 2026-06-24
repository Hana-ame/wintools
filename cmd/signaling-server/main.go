// AI: generated with assistance from AI (2026-06-23)
//
// Signaling Server — WebRTC 房间型信令交换服务器 (Gin)
//
// =====================================================
// 编译与运行
// =====================================================
// go run   ./cmd/signaling-server/
// go build -o signaling-server ./cmd/signaling-server/
// ./signaling-server
package main

import (
	"github.com/Hana-ame/wintools/pkg/signaling"
)

func main() {
	s := signaling.New()
	s.Run(":8080")
}
