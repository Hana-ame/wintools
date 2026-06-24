package main

import (
	"log"
	"os"
	"strconv"

	"github.com/Hana-ame/wintools/test/localdns"
)

func main() {
	dohEndpoint := os.Getenv("DOH_ENDPOINT")
	if dohEndpoint == "" {
		
	}
	port := 5353
	if p := os.Getenv("DNS_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}

	if err := localdns.Run(dohEndpoint, port); err != nil {
		log.Fatalf("localdns: %v", err)
	}
}
