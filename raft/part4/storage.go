package part4

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Storage 存储接口的定义
type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	PersistToFile()
	restoreFromFile() bool
	SnapShot(command []byte)
	getSnapShot() []byte
	HasData() bool
}

const DATAWITHOUTLOG = "dataWithoutLog"

// MapStorage 使用Map进行存储
type MapStorage struct {
	mu               sync.Mutex
	runningFilename  string
	snapShotFilename string
	persistReadyChan chan struct{}
	m                map[string][]byte // 以键值对的方式存储raft集群中的信息
}

// SnapShot 保存数据信息
type SnapShot struct {
	M       map[string][]byte // 以键值对的方式存储raft集群中的信息
	Command []byte            // 存储被丢弃日志的数据
}

// NewMapStorage 创建存储对象
func NewMapStorage(runningFilename, snapShotFilename string, persistReadyChan chan struct{}) *MapStorage {

	m := make(map[string][]byte)
	// 需要两部分数据，已丢弃日志的数据以及集群数据(索引以及日志等信息)
	mapStorage := &MapStorage{
		runningFilename:  runningFilename,
		snapShotFilename: snapShotFilename,
		persistReadyChan: persistReadyChan,
		m:                m,
	}
	return mapStorage
}

func (ms *MapStorage) restoreFromFile() bool {
	// 读取运行数据
	if fileExists(ms.runningFilename) {
		// 文件存在
		file, err := os.Open(ms.runningFilename)
		if err != nil {
			log.Fatal(err)
			return false
		}
		defer file.Close()
		decoder := gob.NewDecoder(file)
		var snapShot SnapShot
		err = decoder.Decode(&snapShot)
		if err != nil && err != io.EOF {
			log.Fatalf("restoreFromFile Error decoding:%v", err)
			return false
		}
		// 将持久化的数据给到ms
		if snapShot.M != nil {
			ms.m = snapShot.M
		}
		return true
	} else {
		return false
	}
}

// PersistToFile 持久化运行数据
func (ms *MapStorage) PersistToFile() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(ms.runningFilename), 0755); err != nil {
		fmt.Println("Error creating directories:", err)
		return
	}
	file, err := os.Create(ms.runningFilename)
	if err != nil {
		log.Fatalf("Running-SnapShot Error:%+v", err)
		return
	}
	defer file.Close()
	snapShot := SnapShot{
		M: ms.m,
	}
	if err = gob.NewEncoder(file).Encode(snapShot); err != nil {
		log.Fatalf("Running-SnapShot Error:%+v", err)
	}
}

func (ms *MapStorage) getSnapShot() []byte {
	ms.mu.Lock()
	snapShotFilename := ms.snapShotFilename
	ms.mu.Unlock()
	// 读取持久化数据
	if fileExists(snapShotFilename) {
		// 文件存在
		file, err := os.Open(snapShotFilename)
		if err != nil {
			log.Fatal(err)
			return nil
		}
		defer file.Close()
		decoder := gob.NewDecoder(file)
		var snapShot SnapShot
		err = decoder.Decode(&snapShot)
		if err != nil && err != io.EOF {
			log.Fatalf("getSnapShot Error decoding:%v", err)
			return nil
		}
		fmt.Printf("load snapShot=%+v, from [%v] \n", snapShot, ms.snapShotFilename)
		return snapShot.Command
	}
	return nil
}

func (ms *MapStorage) SnapShot(command []byte) {
	// 触发持久化
	ms.mu.Lock()
	snapShotFilename := ms.snapShotFilename
	ms.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(snapShotFilename), 0755); err != nil {
		fmt.Println("Error creating directories:", err)
		return
	}
	file, err := os.Create(snapShotFilename)
	if err != nil {
		log.Fatalf("SnapShot Error:%+v", err)
		return
	}
	defer file.Close()
	snapShot := SnapShot{
		Command: command,
	}
	if err = gob.NewEncoder(file).Encode(snapShot); err != nil {
		log.Fatalf("SnapShot Error:%+v", err)
	}

	fmt.Printf("dump snapShot=%+v\n to [%+v]", snapShot, ms.snapShotFilename)
}

// Get 获得数据
func (ms *MapStorage) Get(key string) ([]byte, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	v, found := ms.m[key]
	return v, found

}

// Set 设置数据
func (ms *MapStorage) Set(key string, value []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

// HasData 是否存在数据
func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.m) > 0 || fileExists(ms.runningFilename)
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}
