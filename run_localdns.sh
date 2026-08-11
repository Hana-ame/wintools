#!/bin/bash
set -euo pipefail

DOH_ENDPOINT="${DOH_ENDPOINT:-https://cloudflare-dns.com/dns-query}"

exec go run ./cmd/localdns
