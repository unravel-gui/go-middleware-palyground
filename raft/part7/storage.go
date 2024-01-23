//package part7
//
//import (
//	"encoding/gob"
//	"fmt"
//	"io"
//	"os"
//	"path"
//	"raft/part6/common"
//	"sync"
//)
//
//type RuntimeState struct {
//	Term        int64
//	VotedFor    int64
//	CommitIndex int64
//}
//
//func NewRuntimeState(term, votedFor, commitIndex int64) *RuntimeState {
//	return &RuntimeState{
//		Term:        term,
//		VotedFor:    votedFor,
//		CommitIndex: commitIndex,
//	}
//}
//
//type Storage struct {
//	mu              sync.Mutex
//	path            string
//	name            string
//	snapshotManager *SnapShotManager
//}
//
//// NewStorage 创建新的 Storage 实例
//func NewStorage(filePath string) (*Storage, string, error) {
//	msg := ""
//	if filePath == "" {
//		msg = fmt.Sprintf("Persist path: %s is not exists, create directory\n", filePath)
//	}
//	if !common.IsExisted(filePath) {
//		msg = fmt.Sprintf("Persist path: %s is not exists, create directory\n", filePath)
//		err := os.MkdirAll(filePath, os.ModePerm)
//		if err != nil {
//			return nil, msg, err
//		}
//	} else if !common.IsDir(filePath) {
//		return nil, "", fmt.Errorf("Persist path: %s is not a directory\n", filePath)
//	} else {
//		msg = fmt.Sprintf("Persist path: %s\n", common.GetFullPathName(filePath))
//	}
//	name := "raft-state"
//	return &Storage{
//		path:            filePath,
//		name:            name,
//		snapshotManager: NewSnapShotManager(filePath),
//	}, msg, nil
//}
//
//func (p *Storage) Persist(rs *RuntimeState, logs []LogEntry, snap *SnapShot) error {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	filePath := path.Join(p.path, p.name)
//
//	file, err := os.Create(filePath)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//	if err = gob.NewEncoder(file).Encode(&rs); err != nil && err != io.EOF {
//		return err
//	}
//	if err = gob.NewEncoder(file).Encode(&logs); err != nil && err != io.EOF {
//		return err
//	}
//	err = file.Sync()
//	if err != nil {
//		return err
//	}
//	if snap != nil {
//		return p.snapshotManager.SaveSnap(snap)
//	}
//	return nil
//}
//
//func (p *Storage) LoadRuntimeState() (*RuntimeState, error) {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	filePath := path.Join(p.path, p.name)
//	file, err := os.Open(filePath)
//	if err != nil {
//		return nil, err
//	}
//	defer file.Close()
//	var rs *RuntimeState
//	if err = gob.NewDecoder(file).Decode(&rs); err != nil && err != io.EOF {
//		return nil, err
//	}
//	return rs, nil
//}
//
//func (p *Storage) LoadSnapShot() *SnapShot {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	return p.snapshotManager.LoadSnap()
//}
//
//func (p *Storage) LoadEntries() ([]LogEntry, error) {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	filePath := path.Join(p.path, p.name)
//	file, err := os.Open(filePath)
//	if err != nil {
//		return nil, err
//	}
//	defer file.Close()
//	decoder := gob.NewDecoder(file)
//	var rs *RuntimeState
//	if err = decoder.Decode(&rs); err != nil && err != io.EOF {
//		return nil, err
//	}
//	var ents []LogEntry
//	if err = decoder.Decode(&ents); err != nil && err != io.EOF {
//		return nil, err
//	}
//	return ents, nil
//}
//
//func (p *Storage) GetRaftStateSize() int64 {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	filePath := path.Join(p.path, p.name)
//	file, err := os.Open(filePath)
//	if err != nil {
//		return -1
//	}
//	defer file.Close()
//
//	fileInfo, err := file.Stat()
//	if err != nil {
//		return -1
//	}
//
//	return fileInfo.Size()
//}

package part7

import "sync"

// Storage 存储接口的定义
type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	HasData() bool
}

// MapStorage 使用Map进行存储
type MapStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

// NewMapStorage 创建存储对象
func NewMapStorage() *MapStorage {
	m := make(map[string][]byte)
	return &MapStorage{
		m: m,
	}
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
	return len(ms.m) > 0
}
