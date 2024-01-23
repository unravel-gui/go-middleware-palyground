package part8

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// 测试基本运行
func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 获得leaderId
	origLeaderId, origTerm := h.CheckSingleLeader()
	// 断开leader的网络
	h.DisconnectPeer(origLeaderId)
	// 给足时间选举新Leader
	sleepMs(350)
	// 新一轮LeaderId
	newLeaderId, newTerm := h.CheckSingleLeader()
	// 新Leader节点的Id必然与就旧的不同，旧的还没恢复连接
	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	// 新任期更大，进入选举时任期会+1
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}
}

// 测试Leader节点和大部分节点断联的情况
func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Leader断联
	origLeaderId, _ := h.CheckSingleLeader()
	h.DisconnectPeer(origLeaderId)
	// 一共3个节点断了这个后只剩下一个存活
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)

	// 给足时间选举
	// 由于大多数节点断联，所以无法通过选举，也就不会存在Leader节点
	sleepMs(450)
	// 检查是否不存在Leader节点
	h.CheckNoLeader()
	// 重连一个节点(恢复大多数节点的通信)
	h.ReconnectPeer(otherId)
	// 检查是否只存在一个Leader节点
	h.CheckSingleLeader()
}

// 测试所有节点断开重连
func TestDisconnectAllThenRestore(t *testing.T) {
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

// 测试选举后旧Leader重连的情况
func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)

	sleepMs(350)
	newLeaderId, newTerm := h.CheckSingleLeader()

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

		// Reconnect both.
		h.ReconnectPeer(otherId)
		h.ReconnectPeer(leaderId)

		// Give it time to settle
		sleepMs(150)
	}
}

// 测试提交一条数据并检查是否存在内存泄露
func TestCommitOneCommand(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	tlog("submitting 42 to %d", origLeaderId)
	// 提交一条数据
	isLeader := h.SubmitToServer(origLeaderId, 42)
	if !isLeader {
		t.Errorf("want id=%d leader, but it's not", origLeaderId)
	}

	sleepMs(250)
	// 检查已提交数据
	h.CheckCommittedN(42, 3)
}

// 测试向非Leader节点中提交数据
func TestSubmitNonLeaderFails(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	sid := (origLeaderId + 1) % 3
	tlog("submitting 42 to %d", sid)
	isLeader := h.SubmitToServer(sid, 42)
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

	values := []int{42, 55, 81}
	for _, v := range values {
		tlog("submitting %d to %d", v, origLeaderId)
		// 插入数据
		isLeader := h.SubmitToServer(origLeaderId, v)
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", origLeaderId)
		}
		sleepMs(100)
	}
	// 检查拥有已提交数据的节点数和插入顺序和索引顺序是否相同
	// 插入顺序靠后的索引必定靠后
	sleepMs(250)
	nc, i1 := h.CheckCommitted(42)
	_, i2 := h.CheckCommitted(55)
	if nc != 3 {
		t.Errorf("want nc=3, got %d", nc)
	}
	if i1 >= i2 {
		t.Errorf("want i1<i2, got i1=%d i2=%d", i1, i2)
	}
	_, i3 := h.CheckCommitted(81)
	if i2 >= i3 {
		t.Errorf("want i2<i3, got i2=%d i3=%d", i2, i3)
	}
}

// 测试复杂的提交情况并且检查内存是否泄露
func TestCommitWithDisconnectionAndRecover(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	// 提交一组数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	//  给时间提交
	sleepMs(250)

	h.CheckCommittedN(6, 3)
	// 断开一个非Leader节点
	dPeerId := (origLeaderId + 1) % 3
	h.DisconnectPeer(dPeerId)
	sleepMs(250)
	// 重新提交一条数据
	h.SubmitToServer(origLeaderId, 7)
	sleepMs(250)
	// 检查数据状态
	// 此时断联一个节点，所以节点数是2
	h.CheckCommittedN(7, 2)

	// 断联节点重联
	h.ReconnectPeer(dPeerId)
	sleepMs(250)
	// 检查是否只存在一个Leader
	h.CheckSingleLeader()
	// 给够时间进行日志同步
	sleepMs(2500)
	// 检查数据状态
	// 此时节点已恢复所以是3个节点
	h.CheckCommittedN(7, 3)
}

// 测试
func TestNoCommitWithNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 提交一组数据
	origLeaderId, origTerm := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(250)
	// 检查数据状态
	h.CheckCommittedN(6, 3)

	// 断开大多数非leader节点
	dPeer1 := (origLeaderId + 1) % 3
	dPeer2 := (origLeaderId + 2) % 3
	h.DisconnectPeer(dPeer1)
	h.DisconnectPeer(dPeer2)
	sleepMs(250)

	// 提交数据
	h.SubmitToServer(origLeaderId, 8)
	sleepMs(250)
	// 检查数据是否没有被提交
	// 此时由于大多数节点断联是无法通过提交判断的，所以数据无法被提交成功
	h.CheckNotCommitted(8)

	// 断联节点重连
	h.ReconnectPeer(dPeer1)
	h.ReconnectPeer(dPeer2)
	sleepMs(600)
	// 重连后数据是未被提交的，所以不应该被查到
	h.CheckNotCommitted(8)
	// 由于数据未被提交，所以数据状态是一致的
	// 但是断联节点的term更高，所以旧Leader无法当选新Leader节点
	newLeaderId, againTerm := h.CheckSingleLeader()
	if origTerm == againTerm {
		t.Errorf("got origTerm==againTerm==%d; want them different", origTerm)
	}

	// 重新选举后恢复正常，可以被正常提交
	h.SubmitToServer(newLeaderId, 9)
	h.SubmitToServer(newLeaderId, 10)
	h.SubmitToServer(newLeaderId, 11)
	sleepMs(350)

	for _, v := range []int{9, 10, 11} {
		h.CheckCommittedN(v, 3)
	}
}

// 测试Leader在短暂断网后能否继续使用
func TestDisconnectLeaderBriefly(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// 提交一组数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	sleepMs(250)
	h.CheckCommittedN(6, 3)

	// Leader节点重联
	h.DisconnectPeer(origLeaderId)
	// 小于超时时间不会重选Leader节点
	sleepMs(10)
	h.ReconnectPeer(origLeaderId)
	sleepMs(200)
	// 提交数据给旧Leader节点
	h.SubmitToServer(origLeaderId, 7)
	sleepMs(250)
	h.CheckCommittedN(7, 3)
}

// 测试网络分区导致数据丢失的情况
func TestCommitsWithLeaderDisconnects(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	// 提交一组数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 5)

	// 旧Leader节点断联
	h.DisconnectPeer(origLeaderId)
	sleepMs(10)
	// 在超时选举前向旧Leader节点中提交数据
	// 节点断联但是仍然能够接收到客户端信息
	h.SubmitToServer(origLeaderId, 7)

	// 给时间重新Leader节点
	sleepMs(250)
	// 由于Leader节点断联所以数据不会被提交到集群
	// 所以不存在提交的数据,只有Leader节点的log中存在
	h.CheckNotCommitted(7)
	// 此时整个集群中存在新旧两个Leader，旧Leader由于网络分区永远不会提交数据
	newLeaderId, _ := h.CheckSingleLeader()
	// 新Leader节点正常工作
	h.SubmitToServer(newLeaderId, 8)
	sleepMs(250)
	h.CheckCommittedN(8, 4)

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
	h.SubmitToServer(newLeaderId, 9)
	sleepMs(250)
	h.CheckCommittedN(9, 5)
	h.CheckCommittedN(8, 5)

	// 数据7未被提交，这里体现了脑裂导致数据丢失的问题
	h.CheckNotCommitted(7)
}

// 基础测试节点宕机是否会导致系统崩溃
func TestCrashFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)

	sleepMs(350)
	h.CheckCommittedN(5, 3)
	// 节点宕机
	h.CrashPeer((origLeaderId + 1) % 3)
	sleepMs(350)
	h.CheckCommittedN(5, 2)
}

// 测试Follower宕机后重启的情况
func TestCrashThenRestartFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	// 插入一组测试数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	h.SubmitToServer(origLeaderId, 7)

	vals := []int{5, 6, 7}

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
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	h.SubmitToServer(origLeaderId, 7)

	vals := []int{5, 6, 7}

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
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	h.SubmitToServer(origLeaderId, 7)

	vals := []int{5, 6, 7}

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

	h.SubmitToServer(newLeaderId, 8)
	sleepMs(250)

	vals = []int{5, 6, 7, 8}
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

// 测试复杂的宕机和断联情况
func TestReplaceMultipleLogEntries(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// 插入一组测试数据
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(450)
	h.CheckCommittedN(6, 3)

	// Leader节点失联
	h.DisconnectPeer(origLeaderId)
	sleepMs(10)

	// 向断联Leader节点提交数据
	h.SubmitToServer(origLeaderId, 21)
	sleepMs(5)
	h.SubmitToServer(origLeaderId, 22)
	sleepMs(5)
	h.SubmitToServer(origLeaderId, 23)
	sleepMs(5)
	h.SubmitToServer(origLeaderId, 24)
	sleepMs(5)
	// 获得新Leader节点
	newLeaderId, _ := h.CheckSingleLeader()

	// 向新Leader中提交数据
	h.SubmitToServer(newLeaderId, 8)
	sleepMs(5)
	h.SubmitToServer(newLeaderId, 9)
	sleepMs(5)
	h.SubmitToServer(newLeaderId, 10)
	sleepMs(350)
	// 旧Leader节点失联中所以无法提交
	h.CheckNotCommitted(21)
	// 新Leader节点可以提交成功
	h.CheckCommittedN(10, 2)
	// 新Leader节点宕机后重启
	h.CrashPeer(newLeaderId)
	sleepMs(60)
	h.RestartPeer(newLeaderId)

	sleepMs(100)
	// 获得最终的Leader节点
	finalLeaderId, _ := h.CheckSingleLeader()
	// 恢复旧Leader节点
	h.ReconnectPeer(origLeaderId)
	// 给时间进行日志同步
	sleepMs(400)

	// 向最终Leader节点中提交数据
	h.SubmitToServer(finalLeaderId, 11)
	sleepMs(350)
	// 数据21是旧主节点在网络分区时接收的数据，无法被提交，所以数据中没有
	h.CheckNotCommitted(21)
	// 数据11时折腾后的集群正常提交的
	h.CheckCommittedN(11, 3)
	// 数据10是新Leader节点提交的，可以大多数节点所以能被提交成功
	h.CheckCommittedN(10, 3)
}

// 测试提交数据到Leader后崩溃的情况
func TestCrashAfterSubmit(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	// 向旧Leader节点提交数据后瞬间宕机
	// 数据更新到大多数节点但是未被提交到集群
	h.SubmitToServer(origLeaderId, 5)
	sleepMs(1)
	h.CrashPeer(origLeaderId)
	// 节点宕机后无法收到其他节点同步的信息因此无法更新自己的提交进度
	// 正常情况下收到同步成功的信息后更新当前Leader节点的提交进度在下一次心跳的时候通知集群更新提交进度并提交

	// 新的leader已经被选出，但是旧的未来得及提交的数据确保未被提交到集群中
	sleepMs(10)
	h.CheckSingleLeader()
	sleepMs(300)
	h.CheckNotCommitted(5)

	// 旧Leader重启并重新进行选举后 数据5仍未被提交
	h.RestartPeer(origLeaderId)
	sleepMs(150)
	newLeaderId, _ := h.CheckSingleLeader()
	h.CheckNotCommitted(5)

	// 提交新数据时旧的未被提交的数据被提交到集群
	// 之前的同步的数据在集群其他机器中存在，只是未被提交到集群中
	// 所以在新节点向集群提交数据时会将之前的数据一并提交
	h.SubmitToServer(newLeaderId, 6)
	sleepMs(250)
	h.CheckCommittedN(5, 3)
	h.CheckCommittedN(6, 3)
}
