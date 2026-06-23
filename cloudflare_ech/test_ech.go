//go:build ignore

package main

import (
	"fmt"
	"io"
	"net/http"

	"github.com/Hana-ame/wintools/cloudflare_ech"
)

func main() {
	urls := []string{
		"https://video-cf.twimg.com/favicon.ico",
	}

	cli, err := cloudflare_ech.New()
	if err != nil {
		panic(err)
	}

	for _, u := range urls {
		req, _ := http.NewRequest("GET", u, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0")

		resp, err := cli.Do(req)
		if err != nil {
			fmt.Printf("[FAIL] %s: %v\n", u, err)
			continue
		}

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		resp.Body.Close()
		fmt.Printf("[%s] %s\n  %s…\n", resp.Status, u, string(body))
	}
}
