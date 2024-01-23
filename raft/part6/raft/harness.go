package part6

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)
import _ "net/http/pprof"

var basicHost string
var basicPort int
var basicDir string

func init() {
	basicHost = "127.0.0.1"
	basicPort = 9000
	basicDir = "test"
	resetBasicDir(basicDir)
}

func GetEndpoint(p int) string {
	return fmt.Sprintf("%v:%d", basicHost, basicPort+p)
}

func resetBasicDir(filePath string) {
	// 删除目录及其子目录和文件
	err := os.RemoveAll(filePath)
	if err != nil {
		dlog.Error("Error removing directory: %v", err)
	}

	// 创建目录
	err = os.MkdirAll(filePath, 0755)
	if err != nil {
		dlog.Error("Error creating directory: %v", err)
	}
}

// 初始化
func init() {
	// 设置日志内容
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

// Harness 辅助测试Raft结构
type Harness struct {
	mu      sync.Mutex   // 保证高并发数据安全
	cluster []*Server    // 保存所有节点的服务器对象
	storage []*Persister // 保存所有节点的存储对象

	applyChans []chan ApplyMsg // 所有节点用于传输已提交数据的channel
	commits    [][]ApplyMsg    // 用于保存所有节点已提交的数据

	connected []bool // 保存所有节点的连接连接状态
	alive     []bool // 保存所有节点的存活状态

	n int        // 节点数量
	t *testing.T // 测试 t
}

// NewHarness 新建辅助对象
func NewHarness(t *testing.T, n int) *Harness {
	// 初始化
	ns := make([]*Server, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	applyChans := make([]chan ApplyMsg, n)
	commits := make([][]ApplyMsg, n)
	ready := make(chan interface{})
	storage := make([]*Persister, n)

	// 初始化每个节点的内容
	for i := 0; i < n; i++ {
		// 计算当前节点的其他节点列表
		peerIds := make(map[int64]string, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds[int64(p)] = GetEndpoint(p)
			}
		}
		applyChans[i] = make(chan ApplyMsg, 16)
		storage[i] = NewPersister(fmt.Sprintf("%s/raft-state-%d", basicDir, i))
		// 新建raft服务器
		ns[i] = NewServer(int64(i), GetEndpoint(i), peerIds, storage[i], ready, applyChans[i])
		// 启动raft服务
		ns[i].Serve()
		connected[i] = true
		alive[i] = true
	}
	// 关闭ready channel会发送一个信息到chan中
	// 告知监听ready channel的协程已经准备好了
	close(ready)
	// 统统装入Harness中
	h := &Harness{
		cluster:    ns,
		storage:    storage,
		applyChans: applyChans,
		commits:    commits,
		connected:  connected,
		alive:      alive,
		n:          n,
		t:          t,
	}
	// 开启收集已提交数据的协程
	for i := 0; i < n; i++ {
		go h.collectCommits(i)
	}
	return h
}

// Shutdown 关闭辅助对象
func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	var wg sync.WaitGroup
	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			h.alive[i] = false
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				tlog("node [%v] is begin Shutdown", i)
				h.cluster[i].Shutdown()
				tlog("node [%v] is Shutdown success", i)
			}(i)
		}
	}
	for i := 0; i < h.n; i++ {
		close(h.applyChans[i])
	}
	sleepMs(2000)
	sleepMs(2000)
	tlog("waiting exit")
	//wg.Wait()
	tlog("success exit")
}

// DisconnectPeer 断开单个节点，模拟网络分区
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(int64(id))
		}
	}
	h.connected[id] = false
}

// ReconnectPeer 恢复指定节点，模拟网络恢复
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			if err := h.cluster[id].ConnectToPeer(int64(j), h.cluster[j].endpoint); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(int64(id), h.cluster[id].endpoint); err != nil {
				h.t.Fatal(err)
			}
		}
		h.connected[id] = true
	}
}

// CrashPeer 关闭某个节点，模拟某个节点宕机
func (h *Harness) CrashPeer(id int) {
	tlog("Crash %d", id)
	// 当前节点与其他节点断开连接
	h.DisconnectPeer(id)
	h.alive[id] = false
	// 关机
	h.cluster[id].Shutdown()

	h.mu.Lock()
	// 数据清零
	h.commits[id] = h.commits[id][:0]
	h.mu.Unlock()
}

// RestartPeer 重启节点
func (h *Harness) RestartPeer(id int) {
	if h.alive[id] {
		log.Fatalf("id=%d is alive in RestartPeer", id)
	}
	tlog("Restart %d", id)
	// 计算其他节点列表
	peerIds := make(map[int64]string, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds[int64(p)] = GetEndpoint(p)
		}
	}
	// 重新开启节点的任务和节点的数据
	ready := make(chan interface{})
	h.cluster[id] = NewServer(int64(id), GetEndpoint(id), peerIds, h.storage[id], ready, h.applyChans[id])
	h.cluster[id].Serve()
	h.ReconnectPeer(id)
	close(ready)
	h.alive[id] = true
	sleepMs(20)
}

// CheckNoLeader 检查是否不存在Leader节点
func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].cm.Report()
			if isLeader {
				h.t.Fatalf("server %d leader; want none", i)
			}
		}
	}
}

// CheckSingleLeader 检查是否只存在一个Leader
func (h *Harness) CheckSingleLeader() (int, int64) {
	for t := 0; t < 8; t++ {
		leaderId := -1
		leaderTerm := int64(-1)
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].cm.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}
	// 找不到Leader节点返回-1
	h.t.Fatalf("leader not found")
	return -1, -1
}

// CheckCommitted 检查已提交数据
func (h *Harness) CheckCommitted(cmd string) (nc, index int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// 保存已提交数据的长度
	commitsLen := -1
	// 检查每个节点的已提交数据长度是否相同
	for i := 0; i < h.n; i++ {
		if h.connected[i] { // 节点存在连接
			if commitsLen >= 0 {
				if len(h.commits[i]) != commitsLen {
					h.t.Fatalf("commits[%d] = %v, commitsLen= %d", i, h.commits[i], commitsLen)
				}
			} else {
				commitsLen = len(h.commits[i])
			}
		}
	}
	// 检查每个已提交的数据
	for c := 0; c < commitsLen; c++ {
		var cmdAtc *ApplyMsg
		// 检查每个节点相同索引的数据是否相同
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				cmdOfN := h.commits[i][c]
				if cmdAtc != nil {
					if string(cmdOfN.Data) != string(cmdAtc.Data) {
						h.t.Fatalf("got %v, want %v at h.commits[%d][%d]", cmdOfN, *cmdAtc, i, c)
					}
				} else {
					cmdAtc = &cmdOfN
				}
			}
		}
		// 如果是指定的已提交数据则判断这条数据在每个节点中的日志索引是否相同
		// 计算存在这条数据的节点的个数
		if string(cmdAtc.Data) == cmd {
			index = -1
			nc = 0
			for i := 0; i < h.n; i++ {
				if h.connected[i] {
					if index >= 0 && int(h.commits[i][c].Index) != index {
						h.t.Errorf("got Index=%d, want %d at h.commits[%d][%d]", h.commits[i][c].Index, index, i, c)
					} else {
						index = int(h.commits[i][c].Index)
					}
					nc++
				}
			}
			// 返回存在这条数据的节点数和索引
			return nc, index
		}
	}
	// 找不到返回-1
	h.t.Errorf("cmd=%s not found in commits", cmd)
	return -1, -1
}

// CheckCommittedN 检查提交的节点数据并且节点数是否正确
func (h *Harness) CheckCommittedN(cmd string, n int) {
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheclCommitedN got nc=%d, want %d", nc, n)
	}
}

// CheckNotCommitted 检查未提交的数据
func (h *Harness) CheckNotCommitted(cmd string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 检查所有节点
	for i := 0; i < h.n; i++ {
		if h.connected[i] { // 节点如果连接
			// 检查当前节点每个数据
			for c := 0; c < len(h.commits[i]); c++ {
				gotCmd := string(h.commits[i][c].Data)
				// 如果存在指定数据则报错
				if gotCmd == cmd {
					h.t.Errorf("found %s at commits[%d][%d], expected none", cmd, i, c)
				}
			}
		}
	}
}

// SubmitToServer 提交数据到指定的节点
func (h *Harness) SubmitToServer(serverId int, cmd string) bool {
	command := []byte(cmd)
	return h.cluster[serverId].cm.Submit(command)
}

// 封装打印日志
func tlog(format string, a ...interface{}) {
	format = "[TEST]" + format
	log.Printf(format, a...)
}

// 睡眠函数
func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

// 收集已提交的数据到commits中
func (h *Harness) collectCommits(i int) {
	for c := range h.applyChans[i] {
		h.mu.Lock()
		tlog("collectCommits(%d) got %+v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}
