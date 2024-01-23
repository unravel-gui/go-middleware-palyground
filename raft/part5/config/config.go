package conf

import (
	"encoding/json"
	"fmt"
	"os"
	"raft/part6/common"
)

type Config struct {
	Id         int64  `json:"id"`
	IP         string `json:"ip"`
	Port       int64  `json:"port"`
	Endpoint   string
	Pattern    string `json:"pattern"`
	RpcConfig  `json:"rpc_config"`
	RaftConfig `json:"raft_config"`
}

type RpcConfig struct {
	Retry            int64 `json:"retry"`
	HeartbeatTimeout int   `json:"heartbeat_timeout"`
}

type RaftConfig struct {
	ElectionBaseTime   int `json:"election_base_time"`
	ElectionRandomTime int `json:"election_random_time"`
	HeartbeatTimeout   int `json:"heartbeat_timeout"`
}

var DefaultConfig = NewConfig()

func init() {
	LoadConfig(common.DEFAULT_CONFIG_PATH)
}

func NewConfig() *Config {
	// 默认路径
	cfg := new(Config)
	return cfg
}

func LoadConfig(filePath string) {
	DefaultConfig.LoadConfig(filePath)
}

func (cfg *Config) LoadConfig(filePath string) error {
	if !common.IsExisted(filePath) {
		return fmt.Errorf("config file not existed,path=%v", filePath)
	}

	// 读取配置文件
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open config file err:%v", err)
	}
	// 解析 JSON
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return fmt.Errorf("unmarshalling config err:%v", err)
	}
	cfg.Endpoint = fmt.Sprintf("%s:%d", cfg.IP, cfg.Port)
	return nil
}
