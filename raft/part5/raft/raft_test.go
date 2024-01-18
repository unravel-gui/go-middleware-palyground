package raft

import (
	"github.com/fortytw2/leaktest"
	"os"
	"testing"
	"time"
)

func init() {
	// 模拟多集群模式
	os.Setenv("STANDALONE", "false")
}

// 启动并成功运行raft集群
func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

// 测试Leader节点失联后是否能重选新节点为Leader节点
func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	origLeaderId, origTerm := h.CheckSingleLeader()
	h.DisconnectPeer(origLeaderId)
	sleepMs(350)
	newLeaderId, newTerm := h.CheckSingleLeader()
	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	// 新任期更大，进入选举时任期会+1
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}
}

// 测试大多数节点死亡后不能重选Leader节点的情况
func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.DisconnectPeer(origLeaderId)
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)

	// 给足时间选举
	// 由于大多数节点失联，所以无法通过选举，也就不会存在Leader节点
	sleepMs(450)
	// 检查是否不存在Leader节点
	h.CheckNoLeader()
	// 重连一个节点(恢复大多数节点的通信)
	h.ReconnectPeer(otherId)
	// 检查是否只存在一个Leader节点
	h.CheckSingleLeader()
}

// 测试所有节点失联后重新连接的情况 (无数据)
func TestDisconnectAllThenRestoreWithoutData(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	sleepMs(100)
	for i := 0; i < 3; i++ {
		h.DisconnectPeer(i)
	}
	sleepMs(450)
	h.CheckNoLeader()

	for i := 0; i < 3; i++ {
		h.ReconnectPeer(i)
	}
	h.CheckSingleLeader()
}

// TODO: 测试所有节点失联后重新连接的情况 (无数据)

// 测试选举后Leader失连后重连是否能够正确选举Leader
func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)

	sleepMs(350)
	newLeaderId, newTerm := h.CheckSingleLeader()
	// 重连后会因为任期过低，从Leader变为Follower
	h.ReconnectPeer(origLeaderId)
	sleepMs(150)

	againLeaderId, againTerm := h.CheckSingleLeader()

	if newLeaderId != againLeaderId {
		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

// 测试选举后旧Leader重连的情况增加内存泄露检查
func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(150)
	newLeaderId, newTerm := h.CheckSingleLeader()

	// 重连后会因为任期过低，从Leader变为Follower
	h.ReconnectPeer(origLeaderId)
	sleepMs(150)

	againLeaderId, againTerm := h.CheckSingleLeader()

	if newLeaderId != againLeaderId {
		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

// 检查一个非Leader节点断网后恢复的情况
func TestElectionFollowerComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()
	// 非Leader节点断联，这个节点由于收不到Leader的通信会触发选举
	// 选举会导致任期增加，但是由于只有一个节点从而无法获得大多数节点的同意
	// 这就导致不断超时不断选举，任期不断增加
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)
	time.Sleep(650 * time.Millisecond)
	// 恢复连接后任期全局最高会使得Leader成为Follower，并且同步任期
	// 没有Leader后某个节点超时选举
	// 原先断联的节点任期最高但是数据状态不是最新所以不会赢得选举，而原来的Leader在更新term后会重新赢得选举，并同步term到全集群
	h.ReconnectPeer(otherId)
	sleepMs(150)
	_, newTerm := h.CheckSingleLeader()
	// 新选举后任期一定增加，由此判断是否重新选举
	if newTerm <= origTerm {
		t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
	}
}

// 循环测试选举并检查是否存在内存泄露
func TestElectionDisconnectLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	for cycle := 0; cycle < 5; cycle++ {
		leaderId, _ := h.CheckSingleLeader()

		h.DisconnectPeer(leaderId)
		otherId := (leaderId + 1) % 3
		h.DisconnectPeer(otherId)
		sleepMs(310)
		h.CheckNoLeader()

		h.ReconnectPeer(otherId)
		h.ReconnectPeer(leaderId)

		sleepMs(150)
	}
}

// 测试提交一条数据并检查是否存在内存泄露
func TestCommitOneCommand(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	tlog("submitting 42 to %s", origLeaderId)
	command := newSubmitCommand("42", 1024, 2, []string{"path", "to", "test"})
	// 提交一条数据
	isLeader := h.SubmitToServer(origLeaderId, command)
	if !isLeader {
		t.Errorf("want id=%d leader, but it's not", origLeaderId)
	}

	sleepMs(450)
	// 检查已提交数据
	h.CheckCommittedN("42", 3)
}

// 测试向非Leader节点中提交数据
func TestSubmitNonLeaderFails(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	sid := (origLeaderId + 1) % 3
	tlog("submitting 42 to %s", sid)
	isLeader := h.SubmitToServer(sid, newSubmitCommand("42"))
	if isLeader {
		t.Errorf("want id=%d !leader, but it is", sid)
	}
	sleepMs(10)
}

// 测试复杂的提交情况并检查内容是否泄露
func TestCommitMultipleCommands(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	values := []string{"42", "55", "81"}
	for _, v := range values {
		tlog("submitting %s to %d", v, origLeaderId)
		// 插入数据
		isLeader := h.SubmitToServer(origLeaderId, newSubmitCommand(v))
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", origLeaderId)
		}
		sleepMs(100)
	}
	// 检查拥有已提交数据的节点数和插入顺序和索引顺序是否相同
	// 插入顺序靠后的索引必定靠后
	sleep1s()
	nc := h.CheckCommitted("42")
	h.CheckCommitted("55")
	sleep1s()
	h.CheckCommitted("81")
	if nc != 3 {
		t.Errorf("want nc=3, got %d", nc)
	}
}

// TODO:测试复杂的提交情况并且检查内存是否泄露
func TestCommitWithDisconnectionAndRecover(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	// 提交一组数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, newSubmitCommand("5"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("6"))
	//  给时间提交
	sleep1s()
	h.CheckCommittedN("6", 3)
	// 断开一个非Leader节点
	dPeerId := (origLeaderId + 1) % 3
	h.DisconnectPeer(dPeerId)
	// 给时间断开
	sleepMs(250)
	// 重新提交一条数据
	h.SubmitToServer(origLeaderId, newSubmitCommand("7"))
	sleepMs(1250)
	// 检查数据状态
	// 此时断联一个节点，所以节点数是2
	h.CheckCommittedN("7", 2)

	// 断联节点重联
	h.ReconnectPeer(dPeerId)
	sleepMs(1250)
	// 检查是否只存在一个Leader
	h.CheckSingleLeader()
	// 给够时间进行日志同步
	sleep1s()
	// 检查数据状态
	// 此时节点已恢复所以是3个节点
	h.CheckCommittedN("7", 3)
}

// 测试网络分区导致数据丢失的情况(应该丢失)
func TestCommitsWithLeaderDisconnects(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	// 提交一组数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, newSubmitCommand("5"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("6"))

	sleepMs(250)
	h.CheckCommittedN("6", 5)

	// 旧Leader节点断联
	h.DisconnectPeer(origLeaderId)
	sleepMs(10)
	// 在超时选举前向旧Leader节点中提交数据
	// 节点断联但是仍然能够接收到客户端信息
	h.SubmitToServer(origLeaderId, newSubmitCommand("7"))

	// 给时间重新Leader节点
	sleepMs(250)
	// 由于Leader节点断联所以数据不会被提交到集群
	// 所以不存在提交的数据,只有Leader节点的log中存在
	h.CheckNotCommitted("7")
	// 此时整个集群中存在新旧两个Leader，旧Leader由于网络分区永远不会提交数据
	newLeaderId, _ := h.CheckSingleLeader()
	// 新Leader节点正常工作
	h.SubmitToServer(newLeaderId, newSubmitCommand("8"))
	sleepMs(250)
	h.CheckCommittedN("8", 4)

	// 旧Leader节点重联
	h.ReconnectPeer(origLeaderId)
	// 给时间选举和同步数据等
	sleepMs(600)
	// 获得最终的Leader节点
	// 由于旧Leader节点中数据状态更旧所以不会当选
	finalLeaderId, _ := h.CheckSingleLeader()
	if finalLeaderId == origLeaderId {
		t.Errorf("got finalLeaderId==origLeaderId==%d, want them different", finalLeaderId)
	}

	// 网络恢复后集群正常工作
	h.SubmitToServer(newLeaderId, newSubmitCommand("9"))
	sleepMs(450)
	h.CheckCommittedN("9", 5)
	h.CheckCommittedN("8", 5)

	// 数据7未被提交，这里体现了脑裂导致数据丢失的问题
	h.CheckNotCommitted("7")
}

// 基础测试节点宕机是否会导致系统崩溃
func TestCrashFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, newSubmitCommand("5"))

	sleepMs(350)
	h.CheckCommittedN("5", 3)
	// 节点宕机
	h.CrashPeer((origLeaderId + 1) % 3)
	sleepMs(350)
	h.CheckCommittedN("5", 2)
}

// 测试Follower宕机后重启的情况
func TestCrashThenRestartFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 插入一组测试数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, newSubmitCommand("5"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("6"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("7"))

	vals := []string{"5", "6", "7"}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
	// 非Leader节点宕机
	h.CrashPeer((origLeaderId + 1) % 3)
	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 2)
	}

	// 宕机节点恢复
	h.RestartPeer((origLeaderId + 1) % 3)
	// 给足时间同步日志
	sleepMs(650)
	// 检查数据是否恢复
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

// 测试Leader节点宕机后恢复的情况
func TestCrashThenRestartLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 插入一组测试数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, newSubmitCommand("5"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("6"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("7"))

	vals := []string{"5", "6", "7"}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
	// leader宕机
	h.CrashPeer(origLeaderId)
	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 2)
	}
	// Leader恢复
	h.RestartPeer(origLeaderId)
	sleepMs(550)
	// 数据是否正常
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

// 测试所有节点宕机后重启的情况
func TestCrashThenRestartAll(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, newSubmitCommand("5"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("6"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("7"))

	vals := []string{"5", "6", "7"}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	for i := 0; i < 3; i++ {
		h.CrashPeer((origLeaderId + i) % 3)
	}

	sleepMs(350)

	for i := 0; i < 3; i++ {
		h.RestartPeer((origLeaderId + i) % 3)
	}

	sleepMs(150)
	newLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(newLeaderId, newSubmitCommand("8"))
	sleepMs(450)

	vals = []string{"5", "6", "7", "8"}
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

// TODO:测试复杂的宕机和断联情况
func TestReplaceMultipleLogEntries(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// 插入一组测试数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, newSubmitCommand("5"))
	h.SubmitToServer(origLeaderId, newSubmitCommand("6"))

	sleepMs(250)
	h.CheckCommittedN("6", 3)
	sleepMs(1000)
	// Leader节点失联
	h.DisconnectPeer(origLeaderId)
	sleepMs(10)

	// 向断联Leader节点提交数据
	h.SubmitToServer(origLeaderId, newSubmitCommand("21"))
	sleepMs(5)
	h.SubmitToServer(origLeaderId, newSubmitCommand("22"))
	sleepMs(5)
	h.SubmitToServer(origLeaderId, newSubmitCommand("23"))
	sleepMs(5)
	h.SubmitToServer(origLeaderId, newSubmitCommand("24"))
	sleepMs(5)
	// 获得新Leader节点
	newLeaderId, _ := h.CheckSingleLeader()
	sleepMs(1000)
	// 向新Leader中提交数据
	h.SubmitToServer(origLeaderId, newSubmitCommand("8"))
	sleepMs(5)
	h.SubmitToServer(origLeaderId, newSubmitCommand("9"))
	sleepMs(5)
	h.SubmitToServer(origLeaderId, newSubmitCommand("10"))
	sleepMs(450)
	// 旧Leader节点失联中所以无法提交
	h.CheckNotCommitted("21")
	// 新Leader节点可以提交成功
	h.CheckCommittedN("10", 2)
	// 新Leader节点宕机后重启
	h.CrashPeer(newLeaderId)
	sleepMs(60)
	h.RestartPeer(newLeaderId)

	sleepMs(100)
	// 获得最终的Leader节点
	finalLeaderId1, _ := h.CheckSingleLeader()

	// 恢复旧Leader节点
	h.ReconnectPeer(origLeaderId)
	// 给时间进行日志同步
	sleepMs(1000)
	finalLeaderId, _ := h.CheckSingleLeader()
	if finalLeaderId1 == origLeaderId {
		t.Fatalf("origLeader can't eq to finalLeader1,LeaderId=%+v", origLeaderId)
	}
	// 向最终Leader节点中提交数据
	h.SubmitToServer(finalLeaderId, newSubmitCommand("11"))
	sleepMs(1000)
	sleepMs(1000)
	// 数据21是旧主节点在网络分区时接收的数据，无法被提交，所以数据中没有
	h.CheckNotCommitted("21")
	// 数据11时折腾后的集群正常提交的
	h.CheckCommittedN("11", 3)
	// 数据10是新Leader节点提交的，可以大多数节点所以能被提交成功
	h.CheckCommittedN("10", 3)
}

// TODO:测试提交数据到Leader后崩溃的情况(极端情况)
func TestCrashAfterSubmit(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	// 向旧Leader节点提交数据后瞬间宕机
	// 数据更新到大多数节点但是未被提交到集群
	//h.SubmitToServer(origLeaderId, 5)
	for i := 0; i < h.n; i++ {
		cm := h.cluster[i].cm
		cm.mu.Lock()
		cm.raftLog = append(h.cluster[i].cm.raftLog, LogEntry{Command: newSubmitCommand("5"), Term: cm.currentTerm})
		cm.ac.Increment()
		if i == origLeaderId {
			cm.lastLogTerm = cm.currentTerm
			for j := 0; j < h.n; j++ {
				cm.nextIndex[h.cluster[j].endpoint] = len(cm.raftLog)
			}
		}
		cm.mu.Unlock()
	}
	h.CrashPeer(origLeaderId)
	// 节点宕机后无法收到其他节点同步的信息因此无法更新自己的提交进度
	// 正常情况下收到同步成功的信息后更新当前Leader节点的提交进度在下一次心跳的时候通知集群更新提交进度并提交

	// 新的leader已经被选出，但是旧的未来得及提交的数据确保未被提交到集群中
	sleepMs(10)
	h.CheckSingleLeader()
	sleepMs(300)
	h.CheckNotCommitted("5")

	// 旧Leader重启并重新进行选举后 数据5仍未被提交
	h.RestartPeer(origLeaderId)
	sleepMs(15)
	newLeaderId, _ := h.CheckSingleLeader()
	h.CheckNotCommitted("5")

	// 提交新数据时旧的未被提交的数据被提交到集群
	// 之前的同步的数据在集群其他机器中存在，只是未被提交到集群中
	// 所以在新节点向集群提交数据时会将之前的数据一并提交
	h.SubmitToServer(newLeaderId, newSubmitCommand("6"))
	sleepMs(1000)
	h.CheckCommittedN("5", 3)
	h.CheckCommittedN("6", 3)
}
