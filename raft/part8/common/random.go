package common

import (
	"math/rand"
	"sync"
	"time"
)

var once sync.Once

type RandomIDGenerator struct {
	randEngine *rand.Rand
	dist       *rand.Rand
	mutex      sync.Mutex
}

var GlobalIDGenerator = NewRandomIDGenerator()

// NewRandomIDGenerator 创建一个新的 RandomIDGenerator 实例
func NewRandomIDGenerator() *RandomIDGenerator {
	source := rand.NewSource(time.Now().UnixNano())
	return &RandomIDGenerator{
		randEngine: rand.New(source),
		dist:       rand.New(source),
	}
}

// GenerateID 返回一个随机的 ID
func (g *RandomIDGenerator) GenerateID() int {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	// 生成 [0, math.MaxInt32] 范围内的随机整数
	return g.dist.Intn(1<<31 - 1)
}
