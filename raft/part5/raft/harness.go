package raft

import (
	"fmt"
	"log"
	"raft/part5/config"
	"strconv"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	basicPort = 9000
	basicEndpoint = "127.0.0.1:"
}

var basicEndpoint string
var basicPort int

type Harness struct {
	mu      sync.Mutex    // 保证高并发数据安全
	cluster []*Server     // 保存所有节点的服务器对象
	storage []*MapStorage // 保存所有节点的存储对象

	connected []bool // 保存所有节点的连接连接状态
	alive     []bool // 保存所有节点的存活状态

	n int        // 节点数量
	t *testing.T // 测试 t
}

func NewHarness(t *testing.T, n int) *Harness {
	// 初始化
	ns := make([]*Server, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	ready := make(chan interface{})
	storage := make([]*MapStorage, n)

	basePath := "disk/" + strconv.FormatInt(time.Now().UnixNano(), 10) + "/"
	// 初始化每个节点的内容
	for i := 0; i < n; i++ {
		// 计算当前节点的其他节点列表
		peerEndpoints := make([]string, 0)
		for p := 0; p < n; p++ {
			if p != i {
				endpoints := basicEndpoint + strconv.Itoa(p+basicPort)
				peerEndpoints = append(peerEndpoints, endpoints)
			}
		}
		cfg := &config.RaftConfig{
			Read:          true,
			AcMax:         5,
			PersisTimeout: 150,
			Host:          "127.0.0.1",
			Port:          i + basicPort,
			Endpoint:      basicEndpoint + strconv.Itoa(i+basicPort),
		}
		nodeEndpoint := basicEndpoint + strconv.Itoa(i+9000)
		path := basePath + strconv.Itoa(i) + "/"
		persistReadyChan := make(chan struct{})
		runningFilename := path + "runtime-snapShot"
		snapShotFilename := path + "data-snapShot"
		storage[i] = NewMapStorage(runningFilename, snapShotFilename, persistReadyChan)
		// 新建raft服务器
		ns[i] = NewServer(nodeEndpoint, peerEndpoints, storage[i], ready, cfg)
		// 启动raft服务
		ns[i].Serve()
		alive[i] = true
	}
	// 维护每个节点的连接
	for i := 0; i < n; i++ {
		// 当前节点与其他节点建立连接(获得客户端对象)
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(ns[j].endpoint, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	// 关闭ready channel会发送一个信息到chan中
	// 告知监听ready channel的协程已经准备好了
	close(ready)
	log.Println("close ready")
	// 统统装入Harness中
	h := &Harness{
		cluster:   ns,
		storage:   storage,
		connected: connected,
		alive:     alive,
		n:         n,
		t:         t,
	}
	return h
}

// Shutdown 关闭辅助对象
func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			h.alive[i] = false
			h.cluster[i].Shutdown()
		}
	}
}

// DisconnectPeer 断开节点，模拟网络分区
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(h.cluster[id].endpoint)
		}
	}
	h.connected[id] = false
}

// ReconnectPeer 恢复指定节点，模拟网络恢复
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			if err := h.cluster[id].ConnectToPeer(h.cluster[j].endpoint, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(h.cluster[id].endpoint, h.cluster[id].GetListenAddr()); err != nil {
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
	peerEndpoints := make([]string, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			endpoints := basicEndpoint + strconv.Itoa(p+basicPort)
			peerEndpoints = append(peerEndpoints, endpoints)
		}
	}
	// 重新开启节点的任务和节点的数据
	ready := make(chan interface{})
	cfg := &config.RaftConfig{
		Read:          true,
		AcMax:         5,
		PersisTimeout: 150,
		Host:          "127.0.0.1",
		Port:          id + basicPort,
		Endpoint:      "127.0.0.1:" + strconv.Itoa(id+basicPort),
	}
	nodeEndpoint := basicEndpoint + strconv.Itoa(basicPort+id)
	h.cluster[id] = NewServer(nodeEndpoint, peerEndpoints, h.storage[id], ready, cfg)
	h.cluster[id].Serve()
	h.ReconnectPeer(id)
	close(ready)
	log.Println("close ready")
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
func (h *Harness) CheckSingleLeader() (int, int) {
	for c := 0; c < 8; c++ {
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
						h.t.Fatalf("both %d [%s] and %d [%s] think they're leaders",
							leaderId, h.cluster[leaderId].endpoint, i, h.cluster[i].endpoint)
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
func (h *Harness) CheckCommitted(cmd string) (nc int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// 保存已提交数据的长度
	commitsLen := -1
	tmpIndex := -1
	// 检查每个节点的已提交数据长度是否相同
	for i := 0; i < h.n; i++ {
		if h.connected[i] { // 节点存在连接
			if commitsLen >= 0 {
				if h.cluster[i].cm.FileMap.Length() != commitsLen {
					h.t.Fatalf("got diff length, cmd=%s: want Node[%d].FileMap=%+v, but Node[%d].FileMap=%+v",
						cmd, tmpIndex, h.cluster[tmpIndex].cm.FileMap.Datas(), i, h.cluster[i].cm.FileMap.Datas())
				}
			} else {
				commitsLen = h.cluster[i].cm.FileMap.Length()
				tmpIndex = i
			}
		}
	}
	var hash uint32 = 0
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			if hash != 0 {
				tmpHash := h.cluster[i].cm.FileMap.Hash()
				if hash != tmpHash {
					h.t.Fatalf("got diff: want Node[%d].FileMap=%+v, but Node[%d].FileMap=%+v",
						tmpIndex, h.cluster[tmpIndex].cm.FileMap.Datas(), i, h.cluster[i].cm.FileMap.Datas())
				}
			} else {
				hash = h.cluster[i].cm.FileMap.Hash()
			}
		}
	}
	nc = 0
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, ok := h.cluster[i].cm.FileMap.Get(cmd)
			if ok {
				nc++
			}
		}
	}
	return nc

}

// CheckCommittedN 检查提交的节点数据并且节点数是否正确
func (h *Harness) CheckCommittedN(cmd string, n int) {
	nc := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommitedN cmd=[%s] got nc=%d, want %d", cmd, nc, n)
	}
}

func (h *Harness) CheckNotCommitted(cmd string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 检查所有节点
	for i := 0; i < h.n; i++ {
		if h.connected[i] { // 节点如果连接
			// 检查当前节点每个数据
			command := Command{
				OpType: QUERY_FILE,
				Params: newTestFileMeta(cmd),
			}
			file, err := h.cluster[i].cm.cp.Process(command)
			if err != nil {
				h.t.Errorf("check not commit, get err,endpoint=%v, cmd==%v,err=%v", h.cluster[i].endpoint, cmd, err)
			} else if file != (*FileMeta)(nil) {
				h.t.Errorf("found %s at Node[%d],FileMap=%+v, expected none", cmd, i, h.cluster[i].cm.FileMap.Datas())
			}
		}
	}
}

// SubmitToServer 提交数据到指定的节点
func (h *Harness) SubmitToServer(serverId int, cmd Command) bool {
	return h.cluster[serverId].cm.Submit(cmd)
}

// 封装打印日志
func tlog(format string, a ...interface{}) {
	format = "[TEST]" + format
	log.Printf(format, a...)
}

// 睡眠函数
func sleepMs(n int) {
	//fmt.Printf("sleep %d Millisecond\n", n)
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func sleep1s() {
	sleepMs(1000)
}

func newTestFileMeta(fileHash string, data ...interface{}) *FileMeta {
	fm := &FileMeta{FileHash: fileHash}
	if len(data) > 0 {
		if size, ok := data[0].(int); ok {
			fm.Size = size
		} else {
			fmt.Sprintf("type error,ok=%v,data[0]=%v", ok, data[0])
		}
	}
	if len(data) > 1 {
		if ref, ok := data[1].(int); ok {
			fm.Ref = ref
		} else {
			fmt.Sprintf("type error,ok=%v,data[1]=%v", ok, data[1])
		}
	}
	if len(data) > 2 {
		// 转为字符串数据
		if path, ok := data[2].([]string); ok {
			fm.Path = path
		} else {
			fmt.Sprintf("type error,ok=%v,data[2]=%v", ok, data[2])
		}
		fm.Path = data[2].([]string)
	}
	return fm
}

func newSubmitCommand(cmd string, data ...interface{}) Command {
	fm := &FileMeta{FileHash: cmd}
	if len(data) > 0 {
		if size, ok := data[0].(int); ok {
			fm.Size = size
		} else {
			fmt.Sprintf("type error,ok=%v,data[0]=%v", ok, data[0])
		}
	}
	if len(data) > 1 {
		if ref, ok := data[1].(int); ok {
			fm.Ref = ref
		} else {
			fmt.Sprintf("type error,ok=%v,data[1]=%v", ok, data[1])
		}
	}
	if len(data) > 2 {
		// 转为字符串数据
		if path, ok := data[2].([]string); ok {
			fm.Path = path
		} else {
			fmt.Sprintf("type error,ok=%v,data[2]=%v", ok, data[2])
		}
		fm.Path = data[2].([]string)
	}
	cmds := Command{
		OpType: INSERT_FILE,
		Params: fm,
	}
	return cmds
}
