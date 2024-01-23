package raft

import (
	"kvRaft/conf"
	logger "kvRaft/log"
)

var dlog logger.Logger
var defalutConfig = conf.DefaultConfig

func init() {
	dlog = logger.GetBasicLogger()
}
