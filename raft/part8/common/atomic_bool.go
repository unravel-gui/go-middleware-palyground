package common

import "sync/atomic"

type AtomicBool struct {
	flag atomic.Value
}

func NewAtomicBool(initialValue bool) *AtomicBool {
	var ab AtomicBool
	ab.flag.Store(initialValue)
	return &ab
}

func (ab *AtomicBool) Set(newValue bool) {
	ab.flag.Store(newValue)
}

func (ab *AtomicBool) Get() bool {
	return ab.flag.Load().(bool)
}
