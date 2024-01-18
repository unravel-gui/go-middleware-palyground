package raft

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// FileMeta 文件的元数据
type FileMeta struct {
	FileHash string
	Size     int
	Path     []string
	Ref      int
}

func (fm *FileMeta) String() string {
	return fmt.Sprintf("%+v", *fm)
}

func init() {
	gob.Register(FileMeta{})
	gob.Register(FileMetadataStore{})
}

// FileMetadataStore 文件元数据的内存
type FileMetadataStore struct {
	rw   sync.RWMutex
	Data map[string]*FileMeta
}

func NewFileMetadataStore() *FileMetadataStore {
	fm := new(FileMetadataStore)
	d := make(map[string]*FileMeta)
	fm.Data = d
	return fm
}

func (store *FileMetadataStore) Upsert(fileMeta *FileMeta) bool {
	store.rw.Lock()
	defer store.rw.Unlock()
	oldFileMeta, ok := store.Data[fileMeta.FileHash]
	if !ok {
		fileMeta.Ref = 1
	} else {
		fileMeta.Ref = oldFileMeta.Ref + 1
	}
	store.Data[fileMeta.FileHash] = fileMeta
	return true
}

func (store *FileMetadataStore) Get(fileHash string) (*FileMeta, bool) {
	store.rw.RLock()
	defer store.rw.RUnlock()
	value, ok := store.Data[fileHash]
	return value, ok
}

// Remove 直接删除
func (store *FileMetadataStore) Remove(fileHash string) {
	store.rw.Lock()
	defer store.rw.Unlock()
	delete(store.Data, fileHash)
}

// Delete 考虑引用数量进行删除
func (store *FileMetadataStore) Delete(fileHash string) {
	store.rw.Lock()
	defer store.rw.Unlock()
	fileMeta, ok := store.Data[fileHash]
	if !ok {
		// 不存在则返回
		return
	}
	if fileMeta.Ref-1 <= 0 {
		delete(store.Data, fileHash)
		return
	}
	fileMeta.Ref--
	store.Data[fileHash] = fileMeta
	return
}
func (store *FileMetadataStore) Length() int {
	store.rw.RLock()
	defer store.rw.RUnlock()
	return len(store.Data)
}

func (store *FileMetadataStore) Datas() string {
	store.rw.RLock()
	defer store.rw.RUnlock()
	strs := "{"
	for k, v := range store.Data {
		strs = strs + fmt.Sprintf("{%s= %s},", k, v.FileHash)
	}
	strs = strs + "}"
	return strs
}

// Hash 只基于fileHasn进行计算
func (store *FileMetadataStore) Hash() uint32 {
	store.rw.Lock()
	defer store.rw.Unlock()
	hashed := fnv.New32a()
	keys := make([]string, 0, len(store.Data))

	for key := range store.Data {
		keys = append(keys, key)
	}
	// 避免顺序影响
	sort.Strings(keys)
	for _, key := range keys {
		hashed.Write([]byte(key))
	}
	return hashed.Sum32()
}
