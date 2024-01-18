package common

import (
	"os"
	"strings"
)

const RAFT_HEARTNBEAT_SEND = 50
const RAFT_HEARTNBEAT_TIMEOUT = 250

func Min(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func IsStandalone() bool {
	localTestValue := os.Getenv("STANDALONE")
	if strings.ToLower(localTestValue) == "true" {
		return true
	} else {
		return false
	}
}
