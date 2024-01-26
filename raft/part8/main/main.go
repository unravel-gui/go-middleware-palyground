package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"raft/part8"
	httpServer "raft/part8/http"
)

func main() {
	// 定义一个文件路径的命令行参数，默认值为空字符串
	filePath := flag.String("file", "", "File path")
	// 解析命令行参数
	flag.Parse()
	// 打印解析后的参数值
	fmt.Println("File path:", *filePath)
	conf, err := NewTmpConfig(*filePath)
	if err != nil {
		log.Fatalln(err)
		return
	}
	log.Printf("conf=%+v", conf)
	storage, _, err := part8.NewStorage(conf.FilePath)
	if err != nil {
		log.Fatalln(err)
		return
	}
	s := part8.NewServer(conf.ID, fmt.Sprintf("%s:%d", conf.IP, conf.Port), conf.PeerIds, storage)
	hs := httpServer.NewHTTPServer(conf.ID, fmt.Sprintf("%s:%d", conf.IP, conf.Port+10), s)
	s.Serve()
	hs.Start()
}

type TmpConfig struct {
	ID       int            `json:"id"`
	IP       string         `json:"ip"`
	Port     int            `json:"port"`
	PeerIds  map[int]string `json:"peerIds"`
	FilePath string         `json:"filePath"`
}

func NewTmpConfig(filePath string) (*TmpConfig, error) {
	// 创建 TmpConfig 实例
	cof := &TmpConfig{
		FilePath: filePath,
	}

	fileContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading JSON file: %v", err)
	}

	if err := json.Unmarshal(fileContent, cof); err != nil {
		return nil, fmt.Errorf("error decoding JSON: %v", err)
	}
	return cof, nil
}
