package main

import (
	"flag"
	"log"

	"github.com/Hana-ame/wintools/pkg/localdns"
)

func main() {
	dohEndpoint := flag.String("doh", "https://moonchan.xyz/doh", "DoH endpoint URL")
	port := flag.Int("port", 5353, "listen UDP port")
	flag.Parse()

	if err := localdns.Run(*dohEndpoint, *port); err != nil {
		log.Fatalf("localdns: %v", err)
	}
}
