package part8

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)
import _ "net/http/pprof"

// 初始化
func init() {
	// 设置日志内容
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

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
		tlog("Error removing directory: %v", err)
	}

	// 创建目录
	err = os.MkdirAll(filePath, 0755)
	if err != nil {
		tlog("Error creating directory: %v", err)
	}
}

// Harness 辅助测试Raft结构
type Harness struct {
	mu      sync.Mutex // 保证高并发数据安全
	cluster []*Server  // 保存所有节点的服务器对象
	storage []*Storage // 保存所有节点的存储对象

	commitChans []chan ApplyMsg // 所有节点用于传输已提交数据的channel
	commits     [][]ApplyMsg    // 用于保存所有节点已提交的数据

	connected []bool // 保存所有节点的连接连接状态
	alive     []bool // 保存所有节点的存活状态

	n int        // 节点数量
	t *testing.T // 测试 t
}

// NewHarness 新建辅助对象
func NewHarness(t *testing.T, n int) *Harness {
	// 初始化
	os.Setenv("RaftServerTest", "true")
	ns := make([]*Server, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	//commitChans := make([]chan ApplyMsg, n)
	commits := make([][]ApplyMsg, n)
	storage := make([]*Storage, n)
	timestamp := time.Now().Format("2006-01-02_15-04-05.000")
	// 初始化每个节点的内容
	for i := 0; i < n; i++ {
		// 计算当前节点的其他节点列表
		peerIds := make(map[int]string, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds[p] = GetEndpoint(p)
			}
		}
		var err error

		storage[i], _, err = NewStorage(fmt.Sprintf("%s/%s/raft-state-%d", basicDir, timestamp, i))
		if err != nil {
			tlog("NewStorage err:%v", err)
		}
		//commitChans[i] = make(chan ApplyMsg)
		// 新建raft服务器
		ns[i] = NewServer(i, GetEndpoint(i), peerIds, storage[i])
		// 启动raft服务
		ns[i].Serve()
		alive[i] = true
	}
	// 维护每个节点的连接
	for i := 0; i < n; i++ {
		// 当前节点与其他节点建立连接(获得客户端对象)
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].endpoint)
			}
		}
		connected[i] = true
	}
	// 关闭ready channel会发送一个信息到chan中
	// 告知监听ready channel的协程已经准备好了
	// 统统装入Harness中
	h := &Harness{
		cluster: ns,
		storage: storage,
		//commitChans: commitChans,
		commits:   commits,
		connected: connected,
		alive:     alive,
		n:         n,
		t:         t,
	}
	return h
}

// Shutdown 关闭辅助对象
func (h *Harness) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			tlog("Node[%v] is Shutdown ####################################################################", i)
			h.alive[i] = false
			h.cluster[i].Shutdown()
		}
	}
}

// DisconnectPeer 断开狗哥节点，模拟网络分区
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

// ReconnectPeer 恢复指定节点，模拟网络恢复
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].endpoint); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].endpoint); err != nil {
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
}

// RestartPeer 重启节点
func (h *Harness) RestartPeer(id int) {
	if h.alive[id] {
		log.Fatalf("id=%d is alive in RestartPeer", id)
	}
	tlog("Restart %d", id)
	// 计算其他节点列表
	peerIds := make(map[int]string, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds[p] = GetEndpoint(p)
		}
	}
	// 重新开启节点的任务和节点的数据
	h.cluster[id] = NewServer(id, GetEndpoint(id), peerIds, h.storage[id])
	h.cluster[id].Serve()
	h.ReconnectPeer(id)
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
func (h *Harness) CheckSingleLeader() (int, int) {
	for i := 0; i < 8; i++ {
		leaderId := -1
		leaderTerm := -1
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
func (h *Harness) CheckCommitted(cmd int) (nc, index int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// 计算存在这条数据的节点的个数
	vv := ""
	index = -1
	nc = 0
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			v, ok := h.cluster[i].GetForTest(strconv.Itoa(cmd))
			if !ok {
				h.t.Errorf("Node[%v] lost data [%v]", i, cmd)
			} else if index != -1 && vv != v {
				h.t.Errorf("got cmd[%v] diff:Node[%v]:%v  Node[%v]: %v", cmd, i, v, index, vv)
			} else {
				vv = v
				index = i
				nc++
			}
		}
	}
	if nc != 0 {
		// 返回存在这条数据的节点数和索引
		return nc, index
	}
	// 找不到返回-1
	h.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

// CheckCommittedN 检查提交的节点数据并且节点数是否正确
func (h *Harness) CheckCommittedN(cmd int, n int) {
	tlog("CheckCommittedN, cmd=%v", cmd)
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheclCommitedN got nc=%d, want %d", nc, n)
	}
}

// CheckNotCommitted 检查未提交的数据
func (h *Harness) CheckNotCommitted(cmd int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 检查所有节点
	for i := 0; i < h.n; i++ {
		if h.connected[i] { // 节点如果连接
			v, ok := h.cluster[i].GetForTest(strconv.Itoa(cmd))
			if ok {
				h.t.Errorf("found cmd:%d at Node[%v], expected none,reply=%+v", cmd, i, v)
			}
		}
	}
}

// SubmitToServer 提交数据到指定的节点
func (h *Harness) SubmitToServer(serverId, cmd int) bool {
	tlog("Submit cmd=%v to Node[%v]", cmd, serverId)
	var reply CommandReply
	cmdStr := strconv.Itoa(cmd)
	h.cluster[serverId].Put(cmdStr, cmdStr, &reply)
	if reply.CmdStatus != OK {
		tlog("submit err, cmd=%+v, reply=%+v,", cmdStr, reply)
		return false
	}
	tlog("reply:%v", reply)
	return true
}

func (h *Harness) Get(serverId int, key string) bool {
	tlog("Get Key= %v to Node[%v]", key, serverId)
	var reply CommandReply
	h.cluster[serverId].Get(key, &reply)
	if reply.CmdStatus != OK {
		tlog("Get ERROR Key= %v to Node[%v], reply=%+v", key, serverId, reply)
		return false
	}
	tlog("reply:%v", reply)
	return true
}

func (h *Harness) Put(serverId int, key, value string) bool {
	tlog("Put Key:Value= (%v:%v) to Node[%v]", key, value, serverId)
	var reply CommandReply
	h.cluster[serverId].Put(key, value, &reply)
	if reply.CmdStatus != OK {
		tlog("Put ERROR Key:Value= (%v:%v) to Node[%v], reply=%+v", key, value, serverId, reply)
		return false
	}
	tlog("reply:%v", reply)
	return true
}

func (h *Harness) Delete(serverId int, key string) bool {
	tlog("Delete Key= %v to Node[%v]", key, serverId)
	var reply CommandReply
	h.cluster[serverId].Delete(key, &reply)
	if reply.CmdStatus != OK {
		tlog("Delete ERROR Key= %v to Node[%v], reply=%+v", key, serverId, reply)
		return false
	}
	tlog("reply:%v", reply)
	return true
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
