package config

import (
	"encoding/json"
	"io/ioutil"
)

// RaftConfig 结构体用于存储Raft配置项
type RaftConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Endpoint string
	// 持久化
	AcMax         int
	PersisTimeout int
}

// LoadConfig 函数用于加载配置文件
func LoadConfig(filePath string) (*RaftConfig, error) {
	// 读取配置文件
	fileContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// 解析 JSON 配置
	var config RaftConfig
	err = json.Unmarshal(fileContent, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
