package main

import (
	"log"
	"os"

	"github.com/Hana-ame/wintools/localdns"
)

func main() {
	dohEndpoint := os.Getenv("DOH_ENDPOINT")

	if err := localdns.Run(dohEndpoint); err != nil {
		log.Fatalf("localdns: %v", err)
	}
}
