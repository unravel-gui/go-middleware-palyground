package part6

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
