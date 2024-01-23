package common

import (
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	DEFAULT_CONFIG_PATH     = "E:\\Code\\go\\src\\kvRaft\\conf\\kvServer.json"
	RAFT_HEARTNBEAT_TIMEOUT = 100
)

func SleepMs(n int) {
	time.Sleep(10 * time.Duration(n) * time.Millisecond)
}

func IsExisted(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)
}

func IsDir(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Println("Error checking if path is directory:", err)
		return false
	}
	return fileInfo.IsDir()
}

func GetFullPathName(path string) string {
	return filepath.Clean(path)
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
