package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// RaftConfig 结构体用于存储Raft配置项
type RaftConfig struct {
	Leader   string `json:"leader"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Endpoint string
	// 持久化
	AcMax         int `json:"ac_max""`
	PersisTimeout int `json:"persist_timeout"`
	// 读取
	Read     bool `json:"read"`
	LogLevel int  `json:"log_level"`
}

// LoadConfig 函数用于加载配置文件
func LoadConfig(filePath string) (*RaftConfig, error) {
	// 读取配置文件
	fileContent, err := ioutil.ReadFile("leader.json")
	if err != nil {
		return nil, fmt.Errorf("can't get file,err=%v", err)
	}
	config := RaftConfig{}
	if err := json.Unmarshal(fileContent, config); err != nil {
		return nil, err
	}
	return &config, nil
}
