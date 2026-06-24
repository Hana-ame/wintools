package main

import (
	"log"
	"os"
	"strconv"

	"github.com/Hana-ame/wintools/test/localdns"
	"github.com/Hana-ame/wintools/wintools"
)

func main() {
	// 这个,包括 wintools文件夹,都不需要
	if wintools.Disabled() {
		return
	}

	if os.Getenv("LOCALDNS_DISABLE") != "1" {
		go func() {
			dohEndpoint := os.Getenv("DOH_ENDPOINT")
			port := 5353
			if p := os.Getenv("DNS_PORT"); p != "" {
				if n, err := strconv.Atoi(p); err == nil {
					port = n
				}
			}
			if err := localdns.Run(dohEndpoint, port); err != nil {
				log.Printf("localdns: %v", err)
			}
		}()
	}

	select {}
}
