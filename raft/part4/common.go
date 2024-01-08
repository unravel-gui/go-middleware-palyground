package part4

import (
	"sync/atomic"
)

// AtomicCounter 是一个原子性计数器类型
type AtomicCounter struct {
	value int64
}

// Increment 原子性地增加计数器的值
func (ac *AtomicCounter) Increment() {
	atomic.AddInt64(&ac.value, 1)
}

// Decrement 原子性地减少计数器的值
func (ac *AtomicCounter) Decrement() {
	atomic.AddInt64(&ac.value, -1)
}

// Value 获取计数器的当前值
func (ac *AtomicCounter) Value() int64 {
	return atomic.LoadInt64(&ac.value)
}
