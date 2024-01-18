package raft

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Storage 支持存储到文件
type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	PersistToFile() error
	recoverFromFile() error
	SnapShot(data map[string][]byte) error
	getSnapShot() (map[string][]byte, error)
	HasData() bool
}

// MapStorage 使用Map进行存储
type MapStorage struct {
	mu                  sync.Mutex
	runtimeDataFilename string
	snapShotFilename    string
	persistReadyChan    chan struct{}
	m                   map[string][]byte // 以键值对的方式存储raft集群中的信息
	tmp                 map[string][]byte // 临时存储数据实体
}

// SnapShot 保存数据信息
type SnapShot struct {
	M map[string][]byte // 以键值对的方式存储raft集群中的信息
}

var _ Storage = (*MapStorage)(nil)

// NewMapStorage 创建存储对象
func NewMapStorage(runtimeFilename, snapShotFilename string, persistReadyChan chan struct{}) *MapStorage {
	m := make(map[string][]byte)
	mapStorage := &MapStorage{
		runtimeDataFilename: runtimeFilename,
		snapShotFilename:    snapShotFilename,
		persistReadyChan:    persistReadyChan,
		m:                   m,
	}
	return mapStorage
}

// PersistToFile 持久化运行数据
func (ms *MapStorage) PersistToFile() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(ms.runtimeDataFilename), 0755); err != nil {
		return fmt.Errorf("creating dir of runtime data err: %v", err)

	}
	file, err := os.Create(ms.runtimeDataFilename)
	if err != nil {
		return fmt.Errorf("create runtime data file err:%v", err)

	}
	defer file.Close()
	snapShot := SnapShot{
		M: ms.m,
	}
	if err = gob.NewEncoder(file).Encode(snapShot); err != nil {
		return fmt.Errorf("persisting runtime data Error: %v", err)
	}
	return nil
}

func (ms *MapStorage) recoverFromFile() error {
	// 读取运行数据
	if fileExists(ms.runtimeDataFilename) {
		// 文件存在
		file, err := os.Open(ms.runtimeDataFilename)
		if err != nil {
			return fmt.Errorf("open runtime data file err:%v", err)

		}
		defer file.Close()
		decoder := gob.NewDecoder(file)
		var snapShot SnapShot
		err = decoder.Decode(&snapShot)
		if err != nil && err != io.EOF {
			return fmt.Errorf("recovering runtime data from the file Error: %v", err)
		}
		// 将持久化的数据给到ms
		if snapShot.M != nil {
			ms.m = snapShot.M
		}
		return nil
	} else {
		return fmt.Errorf("the runtime data file does not exist, path=[%s]", ms.runtimeDataFilename)
	}
}

func (ms *MapStorage) SnapShot(data map[string][]byte) error {
	ms.mu.Lock()
	snapShotFilename := ms.snapShotFilename
	ms.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(snapShotFilename), 0755); err != nil {
		return fmt.Errorf("creating dir of snapshot error: %v", err)
	}
	file, err := os.Create(snapShotFilename)
	if err != nil {
		return fmt.Errorf("create snapshot file err:%v", err)
	}
	defer file.Close()
	snapShot := SnapShot{
		M: data,
	}
	if err = gob.NewEncoder(file).Encode(snapShot); err != nil {
		return fmt.Errorf("persisting snapshot Error: %v", err)
	}
	return nil
}

func (ms *MapStorage) getSnapShot() (map[string][]byte, error) {
	ms.mu.Lock()
	snapShotFilename := ms.snapShotFilename
	ms.mu.Unlock()
	if fileExists(snapShotFilename) {
		file, err := os.Open(snapShotFilename)
		if err != nil {
			return nil, fmt.Errorf("open snapshot err:%v", err)
		}
		defer file.Close()
		decoder := gob.NewDecoder(file)
		var snapShot SnapShot
		err = decoder.Decode(&snapShot)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("recovering data from snapshot Error: %v", err)
		}
		return snapShot.M, nil
	} else {
		return nil, fmt.Errorf("snapShot does not exist, path=[%s]", ms.runtimeDataFilename)

	}
}

func (ms *MapStorage) Get(key string) ([]byte, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	v, found := ms.m[key]
	return v, found

}

func (ms *MapStorage) Set(key string, value []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

// HasData 是否存在数据
func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.m) > 0 || fileExists(ms.runtimeDataFilename) || fileExists(ms.snapShotFilename)
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}
