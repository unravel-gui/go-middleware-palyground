package kv

//import (
//	"github.com/fortytw2/leaktest"
//	"testing"
//	"time"
//)
//
//// 测试基本运行
//func TestElectionBasic(t *testing.T) {
//	h := NewHarness(t, 3)
//	defer h.Shutdown()
//
//	h.CheckSingleLeader()
//}
//
//func TestElectionLeaderDisconnect(t *testing.T) {
//	h := NewHarness(t, 3)
//	defer h.Shutdown()
//	// 获得leaderId
//	origLeaderId, origTerm := h.CheckSingleLeader()
//	// 断开leader的网络
//	h.DisconnectPeer(origLeaderId)
//	// 给足时间选举新Leader
//	sleepMs(350)
//	// 新一轮LeaderId
//	newLeaderId, newTerm := h.CheckSingleLeader()
//	// 新Leader节点的Id必然与就旧的不同，旧的还没恢复连接
//	if newLeaderId == origLeaderId {
//		t.Errorf("want new leader to be different from orig leader")
//	}
//	// 新任期更大，进入选举时任期会+1
//	if newTerm <= origTerm {
//		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
//	}
//}
//
//func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
//	h := NewHarness(t, 3)
//	defer h.Shutdown()
//
//	// Leader断联
//	origLeaderId, _ := h.CheckSingleLeader()
//	h.DisconnectPeer(origLeaderId)
//	// 一共3个节点断了这个后只剩下一个存活
//	otherId := (origLeaderId + 1) % 3
//	h.DisconnectPeer(otherId)
//
//	// 给足时间选举
//	// 由于大多数节点断联，所以无法通过选举，也就不会存在Leader节点
//	sleepMs(450)
//	// 检查是否不存在Leader节点
//	h.CheckNoLeader()
//	// 重连一个节点(恢复大多数节点的通信)
//	h.ReconnectPeer(otherId)
//	// 检查是否只存在一个Leader节点
//	h.CheckSingleLeader()
//}
//
//// 测试所有节点断开重连
//func TestDisconnectAllThenRestore(t *testing.T) {
//	h := NewHarness(t, 3)
//	defer h.Shutdown()
//
//	sleepMs(100)
//	for i := 0; i < 3; i++ {
//		h.DisconnectPeer(i)
//	}
//	sleepMs(450)
//	h.CheckNoLeader()
//
//	for i := 0; i < 3; i++ {
//		h.ReconnectPeer(i)
//	}
//	h.CheckSingleLeader()
//}
//
//// 测试选举后旧Leader重连的情况
//func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
//	h := NewHarness(t, 3)
//	defer h.Shutdown()
//	origLeaderId, _ := h.CheckSingleLeader()
//
//	h.DisconnectPeer(origLeaderId)
//
//	sleepMs(350)
//	newLeaderId, newTerm := h.CheckSingleLeader()
//
//	h.ReconnectPeer(origLeaderId)
//	sleepMs(150)
//
//	againLeaderId, againTerm := h.CheckSingleLeader()
//
//	if newLeaderId != againLeaderId {
//		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeaderId)
//	}
//	if againTerm != newTerm {
//		t.Errorf("again term got %d; want %d", againTerm, newTerm)
//	}
//}
//
//// 测试选举后旧Leader重连的情况增加内存泄露检查
//func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
//	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()
//
//	h := NewHarness(t, 5)
//	defer h.Shutdown()
//
//	origLeaderId, _ := h.CheckSingleLeader()
//
//	h.DisconnectPeer(origLeaderId)
//	sleepMs(150)
//	newLeaderId, newTerm := h.CheckSingleLeader()
//
//	h.ReconnectPeer(origLeaderId)
//	sleepMs(150)
//
//	againLeaderId, againTerm := h.CheckSingleLeader()
//
//	if newLeaderId != againLeaderId {
//		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeaderId)
//	}
//	if againTerm != newTerm {
//		t.Errorf("again term got %d; want %d", againTerm, newTerm)
//	}
//}
//
//// 检查一个非Leader节点断网后恢复的情况
//func TestElectionFollowerComesBack(t *testing.T) {
//	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()
//
//	h := NewHarness(t, 3)
//	defer h.Shutdown()
//
//	origLeaderId, origTerm := h.CheckSingleLeader()
//	// 非Leader节点断联，这个节点由于收不到Leader的通信会触发选举
//	// 选举会导致任期增加，但是由于只有一个节点从而无法获得大多数节点的同意
//	// 这就导致不断超时不断选举，任期不断增加
//	otherId := (origLeaderId + 1) % 3
//	h.DisconnectPeer(otherId)
//	time.Sleep(650 * time.Millisecond)
//	// 恢复连接后任期全局最高会使得Leader成为Follower，并且同步任期
//	// 没有Leader后某个节点超时选举
//	// 原先断联的节点任期最高但是数据状态不是最新所以不会赢得选举，而原来的Leader在更新term后会重新赢得选举，并同步term到全集群
//	h.ReconnectPeer(otherId)
//	sleepMs(150)
//	_, newTerm := h.CheckSingleLeader()
//	// 新选举后任期一定增加，由此判断是否重新选举
//	if newTerm <= origTerm {
//		t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
//	}
//}
//
//// 循环测试选举并检查是否存在内存泄露
//func TestElectionDisconnectLoop(t *testing.T) {
//	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()
//
//	h := NewHarness(t, 3)
//	defer h.Shutdown()
//
//	for cycle := 0; cycle < 5; cycle++ {
//		leaderId, _ := h.CheckSingleLeader()
//
//		h.DisconnectPeer(leaderId)
//		otherId := (leaderId + 1) % 3
//		h.DisconnectPeer(otherId)
//		sleepMs(310)
//		h.CheckNoLeader()
//
//		// Reconnect both.
//		h.ReconnectPeer(otherId)
//		h.ReconnectPeer(leaderId)
//
//		// Give it time to settle
//		sleepMs(150)
//	}
//}
//
//// 测试提交一条数据并检查是否存在内存泄露
//func TestCommitOneCommand(t *testing.T) {
//	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()
//
//	h := NewHarness(t, 3)
//	defer h.Shutdown()
//
//	origLeaderId, _ := h.CheckSingleLeader()
//
//	tlog("submitting 42 to %d", origLeaderId)
//	// 提交一条数据
//	isLeader := h.SubmitToServer(origLeaderId, 42)
//	if !isLeader {
//		t.Errorf("want id=%d leader, but it's not", origLeaderId)
//	}
//
//	sleepMs(250)
//	// 检查已提交数据
//	h.CheckCommittedN(42, 3)
//}
