package part4

import (
	"sync/atomic"
)

// AtomicCounter 是一个带有最大计数值的原子性计数器类型
type AtomicCounter struct {
	value    int64
	maxCount int64
}

// NewAtomicCounter 创建一个带有最大计数值的原子性计数器
func NewAtomicCounter(maxCount int64) *AtomicCounter {
	return &AtomicCounter{
		maxCount: maxCount,
	}
}

// Increment 原子性地增加计数器的值，达到最大计数值时清零重新开始计数
func (ac *AtomicCounter) Increment() bool {
	newValue := atomic.AddInt64(&ac.value, 1)

	// 如果达到最大计数值，则清零
	if newValue >= ac.maxCount {
		atomic.StoreInt64(&ac.value, 0)
		return true
	}
	return false
}

// Value 获取计数器的当前值
func (ac *AtomicCounter) Value() int64 {
	return atomic.LoadInt64(&ac.value)
}

// SetValue 设置计数器的值
func (ac *AtomicCounter) SetValue(newValue int64) {
	atomic.StoreInt64(&ac.value, newValue)
}
