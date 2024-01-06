package part2

import (
	"log"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	mu          sync.Mutex
	cluster     []*Server
	commitChans []chan CommitEntry

	commits [][]CommitEntry

	connected []bool
	n         int
	t         *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	ready := make(chan interface{})

	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}
		commitChans[i] = make(chan CommitEntry)
		ns[i] = NewServer(i, peerIds, ready, commitChans[i])
		ns[i].Serve()
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	h := &Harness{
		cluster:     ns,
		commitChans: commitChans,
		commits:     commits,
		connected:   connected,
		n:           n,
		t:           t,
	}
	for i := 0; i < n; i++ {
		// 启动协程接收对应节点的收集提交数据
		go h.collectCommits(i)
	}
	return h
}

func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
	for i := 0; i < h.n; i++ {
		close(h.commitChans[i])
	}
}

func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for i := 0; i < h.n; i++ {
		if i != id {
			h.cluster[i].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id {
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

func (h *Harness) CheckSingleLeader() (int, int) {
	for r := 0; r < 8; r++ {
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
	h.t.Fatal("leader not found")
	return -1, -1
}

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

// CheckCommitted 检查已提交数据的正确性以及获得cmd这条数据的节点数量和索引位置
func (h *Harness) CheckCommitted(cmd int) (nc int, index int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	commitLen := -1
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			if commitLen >= 0 {
				// 提交的日志长度必须相等
				if len(h.commits[i]) != commitLen {
					h.t.Fatalf("commits[%d] = %d, commitsLen=%d", i, h.commits[i], commitLen)
				}
			} else {
				commitLen = len(h.commits[i])
			}
		}
	}

	for c := 0; c < commitLen; c++ {
		cmdAtc := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				cmdOfN := h.commits[i][c].Command.(int)
				if cmdAtc >= 0 {
					// 提交的数据必须相同
					if cmdOfN != cmdAtc {
						h.t.Errorf("got %d, want %d at h.commits[%d][%d]", cmdOfN, cmdAtc, i, c)
					}
				} else {
					// 记录提交的数据
					cmdAtc = cmdOfN
				}
			}
		}
		// 如果时目标命令
		if cmdAtc == cmd {
			index := -1
			nc := 0
			for i := 0; i < h.n; i++ {
				if h.connected[i] {
					// 这条已提交数据的索引在每个节点中必须是相同的
					if index >= 0 && h.commits[i][c].Index != index {
						h.t.Errorf("got Index=%d, want %d at h.commits[%d][%d]", h.commits[i][c].Index, index, i, c)
					} else {
						// 记录当前目标数据的索引
						index = h.commits[i][c].Index
					}
					// 存在这条已提交数据的节点数量，如果不满足则会在if语句中抛出错误
					nc++
				}
			}
			// 返回
			return nc, index
		}
	}
	// 未找到
	h.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

// CheckCommittedN 检查提交的数据是否正常
func (h *Harness) CheckCommittedN(cmd int, n int) {
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

// CheckNotCommitted 检查未提交的数据
func (h *Harness) CheckNotCommitted(cmd int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			for c := 0; c < len(h.commits[i]); c++ {
				gotcmd := h.commits[i][c].Command.(int)
				// 存在未提交的数据则出错
				if gotcmd == cmd {
					h.t.Errorf("found %d at commits[%d][%d], expected none", cmd, i, c)
				}
			}
		}
	}
}

// SubmitToServer 向leader节点提交数据
func (h *Harness) SubmitToServer(serverId int, cmd interface{}) bool {
	return h.cluster[serverId].cm.Submit(cmd)
}

func tlog(format string, a ...interface{}) {
	format = "[TEST]" + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

// 收集提交的数据, 将数据保存到commits中
func (h *Harness) collectCommits(i int) {
	for c := range h.commitChans[i] {
		h.mu.Lock()
		tlog("collectionCommits(%d) got %+v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}
