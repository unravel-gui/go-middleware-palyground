package part8

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path"
	"raft/part8/common"
	"sync"
)

type RuntimeState struct {
	Term        int
	VotedFor    int
	CommitIndex int
	Logs        []LogEntry
}

func NewRuntimeState(term, votedFor, commitIndex int) *RuntimeState {
	return &RuntimeState{
		Term:        term,
		VotedFor:    votedFor,
		CommitIndex: commitIndex,
	}
}

type Storage struct {
	mu              sync.Mutex
	path            string
	name            string
	snapshotManager *SnapShotManager
}

// NewStorage 创建新的 Storage 实例
func NewStorage(filePath string) (*Storage, string, error) {
	msg := ""
	if filePath == "" {
		msg = fmt.Sprintf("Persist path: %s is not exists, create directory\n", filePath)
	}
	if !common.IsExisted(filePath) {
		msg = fmt.Sprintf("Persist path: %s is not exists, create directory\n", filePath)
		err := os.MkdirAll(filePath, os.ModePerm)
		if err != nil {
			return nil, msg, err
		}
	} else if !common.IsDir(filePath) {
		return nil, "", fmt.Errorf("Persist path: %s is not a directory\n", filePath)
	} else {
		msg = fmt.Sprintf("Persist path: %s\n", common.GetFullPathName(filePath))
	}
	name := "raft-state"
	return &Storage{
		path:            filePath,
		name:            name,
		snapshotManager: NewSnapShotManager(filePath),
	}, msg, nil
}

func (p *Storage) Persist(rs *RuntimeState, snap *SnapShot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	filePath := path.Join(p.path, p.name)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = gob.NewEncoder(file).Encode(&rs); err != nil && err != io.EOF {
		return err
	}
	err = file.Sync()
	if err != nil {
		return err
	}
	if snap != nil {
		return p.snapshotManager.SaveSnap(snap)
	}
	return nil
}

func (p *Storage) LoadRuntimeState() (*RuntimeState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	filePath := path.Join(p.path, p.name)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var rs *RuntimeState
	if err = gob.NewDecoder(file).Decode(&rs); err != nil && err != io.EOF {
		return nil, err
	}
	return rs, nil
}

func (p *Storage) LoadSnapShot() *SnapShot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotManager.LoadSnap()
}

func (p *Storage) GetRaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	filePath := path.Join(p.path, p.name)
	file, err := os.Open(filePath)
	if err != nil {
		return -1
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return -1
	}

	return int(fileInfo.Size())
}
