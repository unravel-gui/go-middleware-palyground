package common

const RAFT_HEARTNBEAT_TIMEOUT = 50

func Min(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
