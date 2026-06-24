package wintools

import (
	"log"
	"os"
)

// Disabled returns true when WINTOLS_DISABLE=1.
// All service packages should check this at the top of Run().
func Disabled() bool {
	if os.Getenv("WINTOLS_DISABLE") == "1" {
		log.Println("WINTOLS_DISABLE=1, skipping")
		return true
	}
	return false
}
