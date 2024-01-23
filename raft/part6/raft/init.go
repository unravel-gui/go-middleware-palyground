package part6

import (
	conf "raft/part6/config"
	logger "raft/part6/log"
)

var dlog logger.Logger
var defalutConfig = conf.DefaultConfig

func init() {
	dlog = logger.GetBasicLogger()
}
