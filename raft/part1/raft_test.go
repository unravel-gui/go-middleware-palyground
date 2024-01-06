package part1

import (
	"github.com/fortytw2/leaktest"
	"os"
	"testing"
	"time"
)

func init() {
	os.Setenv("logfile", "E:\\\\temp\\rlog")
}

// 测试基础服务
func TestElectionBasic(t *testing.T) {
	// 运行raft集群服务
	h := NewHarness(t, 3)
	// 结束时关闭集群
	defer h.Shutdown()
	// 检查集群中是否只有单个leader节点
	h.CheckSingleLeader()
}

// 测试leader节点断联，是否成功进行选举
func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 获得原LeaderId和任期
	orginLeaderId, orginTerm := h.CheckSingleLeader()
	// leader节点断开连接
	h.DisconnectPeer(orginLeaderId)
	// 给时间进行新leader选举
	sleepMs(350)
	// 检查是否只有一个leader节点
	newLeaderId, newTerm := h.CheckSingleLeader()
	// 节点应该不同，旧leader节点仍在失联中...
	if newLeaderId == orginLeaderId {
		t.Errorf("want new leader tobe different frim orig leader")
	}
	// 新的任期因该更大
	if newTerm <= orginTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, orginTerm)
	}
}

// 测试leader节点与大部分节点失联的情况
func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 旧leader节点和另一个peer节点失联
	orgLeaderId, _ := h.CheckSingleLeader()
	h.DisconnectPeer(orgLeaderId)
	otherId := (orgLeaderId + 1) % 3
	h.DisconnectPeer(otherId)
	// 给足时间重选节点
	sleepMs(450)
	// 由于只剩下一个节点是无法选出新leader节点的，所以此时集群中应该不存在节点
	// 检查集群中是否不存在节点
	h.CheckNoLeader()
	// 另一个节点重连
	h.ReconnectPeer(otherId)
	// 此时足够选出新leader节点，但是也只能存在一个leader节点
	h.CheckSingleLeader()
}

// 所有节点断联并重启
func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	sleepMs(100)
	for i := 0; i < 3; i++ {
		h.DisconnectPeer(i)
	}
	sleepMs(450)
	// 所有节点失联，不存在leader节点
	h.CheckNoLeader()
	for i := 0; i < 3; i++ {
		h.ReconnectPeer(i)
	}
	h.CheckSingleLeader()
}

// 测试换leader后旧leader恢复连接的情况
func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 旧leader节点断联
	origLeaderId, _ := h.CheckSingleLeader()
	h.DisconnectPeer(origLeaderId)
	// 选出新leader节点
	sleepMs(350)
	newLeaderId, newTerm := h.CheckSingleLeader()
	// 旧leader节点恢复连接
	h.ReconnectPeer(origLeaderId)
	sleepMs(150)
	// 再次检查leader节点
	againLeaderId, againTerm := h.CheckSingleLeader()
	// 原先的状态是稳定的如果节点出现不一致则说明集群存在问题
	if newLeaderId != againLeaderId {
		t.Errorf("again leader id got %d;want %d", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
	// leaktest.CheckTimeout用于测试是否存在内存泄露
	// 如果测试函数执行时间超过 100 毫秒，并且仍然存在未释放的 goroutine 或资源，则会在测试失败时报告内存泄漏。
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)
	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 旧leader节点失联
	origLeaderId, _ := h.CheckSingleLeader()
	h.DisconnectPeer(origLeaderId)
	// 获得新选的leader节点
	sleepMs(150)
	newLeaderId, newTerm := h.CheckSingleLeader()
	// 旧leader恢复连接
	h.ReconnectPeer(origLeaderId)
	sleepMs(150)
	// 再次获得leader节点信息
	againLeaderId, againTerm := h.CheckSingleLeader()
	// 如果状态不一致则存在问题
	if newLeaderId != againLeaderId {
		t.Errorf("again leader id got %d;want %d", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

// 测试单个非leader节点失联的情况
func TestElectionFollowerComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)
	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 获得旧leader节点
	origLeaderId, origTerm := h.CheckSingleLeader()
	// 非leader节点失联一段时间后重连
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)
	time.Sleep(650 * time.Millisecond)
	h.ReconnectPeer(otherId)
	sleepMs(150)
	// 失联节点在失联阶段会进行疯狂重选，但是因为无法获得大多数节点的投票无法成为leader
	// 然后继续触发选举流程，term不断增高
	// 在恢复连接后任期为全场最高，原来的leader收到他的回复后会变成follower，然后某个节点会因为没有leader发送心跳从而触发选举
	// 触发选举后则会由任期最高的节点当选
	_, newTerm := h.CheckSingleLeader()
	// 新任期已经经过选举所以必然将高于原来的任期
	if newTerm <= origTerm {
		t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
	}
}

// 测试不断断联再重联，判断是否存在内存溢出
func TestElectionDisconnectLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	h := NewHarness(t, 3)
	defer h.Shutdown()
	for cycle := 0; cycle < 5; cycle++ {
		// leader和大多数节点失联
		leaderId, _ := h.CheckSingleLeader()
		h.DisconnectPeer(leaderId)
		otherId := (leaderId + 1) % 3
		h.DisconnectPeer(otherId)
		sleepMs(310)
		// 大多数节点死亡则无法选出新leader
		h.CheckNoLeader()
		// 失联节点重连
		h.ReconnectPeer(otherId)
		h.ReconnectPeer(leaderId)

		sleepMs(150)
	}
}
