package raft

import (
	"sync"
)

// FileMeta 文件的元数据
type FileMeta struct {
	FileHash string
	Size     int
	Path     []string
	Ref      int
}

// FileMetadataStore 文件元数据的内存
type FileMetadataStore struct {
	rw   sync.RWMutex
	data map[string]*FileMeta
}

func NewFileMetadataStore() *FileMetadataStore {
	fm := new(FileMetadataStore)
	d := make(map[string]*FileMeta)
	fm.data = d
	return fm
}

func (store *FileMetadataStore) Upsert(fileMeta *FileMeta) bool {
	store.rw.Lock()
	defer store.rw.Unlock()
	oldFileMeta, ok := store.data[fileMeta.FileHash]
	if !ok {
		// 不存在则返回
		fileMeta.Ref = 1
	} else {
		fileMeta.Ref = oldFileMeta.Ref + 1
	}
	store.data[fileMeta.FileHash] = fileMeta
	return true
}

func (store *FileMetadataStore) Get(fileHash string) (*FileMeta, bool) {
	store.rw.RLock()
	defer store.rw.RUnlock()
	value, ok := store.data[fileHash]
	return value, ok
}

// Remove 直接删除
func (store *FileMetadataStore) Remove(fileHash string) {
	store.rw.Lock()
	defer store.rw.Unlock()
	delete(store.data, fileHash)
}

// Delete 考虑引用数量进行删除
func (store *FileMetadataStore) Delete(fileHash string) {
	store.rw.Lock()
	defer store.rw.Unlock()
	fileMeta, ok := store.data[fileHash]
	if !ok {
		// 不存在则返回
		return
	}
	if fileMeta.Ref-1 <= 0 {
		delete(store.data, fileHash)
		return
	}
	fileMeta.Ref--
	store.data[fileHash] = fileMeta
	return
}
func (store *FileMetadataStore) Length() int {
	store.rw.RLock()
	defer store.rw.RUnlock()
	return len(store.data)
}
