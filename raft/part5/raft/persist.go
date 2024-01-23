package raft

import (
	"encoding/gob"
	"io"
	"kvRaft/common"
	"os"
	"path"
	"sync"
)

type RuntimeState struct {
	Term        int64
	VotedFor    int64
	CommitIndex int64
}

func NewRuntimeState(term, votedFor, commitIndex int64) *RuntimeState {
	return &RuntimeState{
		Term:        term,
		VotedFor:    votedFor,
		CommitIndex: commitIndex,
	}
}

type Persister struct {
	mu              sync.Mutex
	path            string
	name            string
	snapshotManager *SnapShotManager
}

// NewPersister 创建新的 Persister 实例
func NewPersister(filePath string) *Persister {
	if filePath == "" {
		dlog.Warn("Persist path: %s is not exists, create directory\n", filePath)
	}
	if !common.IsExisted(filePath) {
		dlog.Warn("Persist path: %s is not exists, create directory\n", filePath)
		err := os.MkdirAll(filePath, os.ModePerm)
		if err != nil {
			dlog.Error("Failed to create directory:", err)
		}
	} else if !common.IsDir(filePath) {
		dlog.Error("Persist path: %s is not a directory\n", filePath)
	} else {
		dlog.Info("Persist path: %s\n", common.GetFullPathName(filePath))
	}
	name := "raft-state"
	return &Persister{
		path:            filePath,
		name:            name,
		snapshotManager: NewSnapShotManager(filePath),
	}
}

func (p *Persister) Persist(rs *RuntimeState, logs []LogEntry, snap *SnapShot) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	filePath := path.Join(p.path, p.name)
	file, err := os.Create(filePath)
	if err != nil {
		dlog.Error("persist open file err: %v", err)
		return false
	}

	if err = gob.NewEncoder(file).Encode(&rs); err != nil && err != io.EOF {
		dlog.Error("encoding RuntimeState err: %v", err)
		return false
	}
	if err = gob.NewEncoder(file).Encode(&logs); err != nil && err != io.EOF {
		dlog.Error("encoding logs err: %v", err)
		return false
	}
	file.Sync()
	file.Close()
	if snap != nil {
		return p.snapshotManager.SaveSnap(snap)
	}
	return true
}

func (p *Persister) LoadRuntimeState() *RuntimeState {
	p.mu.Lock()
	defer p.mu.Unlock()
	filePath := path.Join(p.path, p.name)
	file, err := os.Open(filePath)
	if err != nil {
		dlog.Error("open file err: %v", err)
		return nil
	}
	defer file.Close()
	var rs *RuntimeState
	if err = gob.NewDecoder(file).Decode(&rs); err != nil && err != io.EOF {
		dlog.Error("Error unmarshalling RuntimeState:", err)
		return nil
	}
	return rs
}

func (p *Persister) LoadSnapShot() *SnapShot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotManager.LoadSnap()
}

func (p *Persister) LoadEntries() []LogEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	filePath := path.Join(p.path, p.name)
	file, err := os.Open(filePath)
	if err != nil {
		dlog.Error("open file err: %v", err)
		return nil
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	var rs *RuntimeState
	if err = decoder.Decode(&rs); err != nil && err != io.EOF {
		dlog.Error("Error unmarshalling RuntimeState:", err)
		return nil
	}
	var ents []LogEntry
	if err = decoder.Decode(&ents); err != nil && err != io.EOF {
		dlog.Error("Error unmarshalling Logs:%v", err)
		return nil
	}
	return ents
}

func (p *Persister) GetRaftStateSize() int64 {
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

	return fileInfo.Size()
}
