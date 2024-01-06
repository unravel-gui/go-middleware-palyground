package part1

import (
	"log"
	"testing"
	"time"
)

func init() {
	// log.Ltime: 表示在每条日志消息前面添加时间信息，格式为HH:MM:SS。
	// log.Lmicroseconds: 表示在时间信息中添加微秒级别的精确时间，格式为HH:MM:SS.ffffff。
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	cluster []*Server // 服务器对象

	connected []bool // 连接状态

	n int // 节点数量
	t *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
	// 存放所有服务器
	ns := make([]*Server, n)
	// 存放所有的连接对象
	connected := make([]bool, n)
	// 用于通知是否准备好的channel
	ready := make(chan interface{})

	// 开启每个raft节点
	for i := 0; i < n; i++ {
		// 存放peer节点id
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i { // 除自己外的id存入peerIds
				peerIds = append(peerIds, p)
			}
		}
		// 创建server对象
		ns[i] = NewServer(i, peerIds, ready)
		// 启动server服务
		ns[i].Serve()
	}
	// 建立每个raft节点与其他peer节点间的连接
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				// 建立连接，获得客户端
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		// 标记当前id为i的节点与其他peer节点建立完毕
		connected[i] = true
	}
	// 每个节点的服务都已经启动了，并且每个节点都拥有对象的rpc客户端
	// 通知准备工作完成
	close(ready) // 关闭chan时会发送一个零值
	// 封装调用对象
	return &Harness{
		cluster:   ns,
		connected: connected,
		n:         n,
		t:         t,
	}
}

// Shutdown 停止
func (h *Harness) Shutdown() {
	// 关闭节点间的所有连接
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	// 关闭每个节点的服务
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
}

// DisconnectPeer 断开这个节点与其他节点的通信
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	// 关闭这个节点拥有的与其他节点的客户端连接
	h.cluster[id].DisconnectAll()
	// 关闭其他节点拥有的这个节点的客户端连接
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(id)
		}
	}
	// 标记当前节点断联
	h.connected[id] = false
}

// ReconnectPeer 恢复peer节点的通信
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id {
			// 当前节点获得其他节点的客户端(通信)
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			// 其他节点获得当前节点的客户端(通信)
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	// 标记节点恢复通信
	h.connected[id] = true
}

// CheckSingleLeader 检查是否只有单个Leader节点
func (h *Harness) CheckSingleLeader() (int, int) {
	for r := 0; r < 5; r++ {
		leaderId := -1
		leaderTerm := -1
		// 检查每个节点
		for i := 0; i < h.n; i++ {
			// 保持通信则进行判断
			if h.connected[i] {
				// 获得节点信息
				_, term, isLeader := h.cluster[i].cm.Report()
				if isLeader { // 如果是leader节点进入判断
					if leaderId < 0 {
						// 这是第一个leader节点，保存leader节点和任期信息
						leaderId = i
						leaderTerm = term
					} else {
						// 存在两个leader节点,出错
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		// 检查完每个节点，如果存在leader节点则返回
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		// 延迟时间给集群时间选出新leader节点
		time.Sleep(150 * time.Millisecond)
	}
	// 尝试五次没有leader节点，集群出现问题
	h.t.Fatalf("leader not found")
	return -1, -1
}

// CheckNoLeader 检查是否没有leader节点
func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].cm.Report()
			if isLeader { // 如果存在leader节点则报错
				h.t.Fatalf("server %d leader; want none", i)
			}
		}
	}
}

// 封装打印信息
func tlog(format string, a ...interface{}) {
	format = "[TEST]" + format
	log.Printf(format, a...)
}

// 执行延迟时间
func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
