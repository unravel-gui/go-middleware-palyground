package part8

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"raft/part8/common"
	"sync"
	"time"
)

const DebugCM = 1

type CMState int

const (
	Follower  CMState = iota // 跟随着，默认初始状态
	Candidate                // 候选人，当触发选举时节点将转换未Candidate开始拉票
	Leader                   // 领导者，用于接收用户请求，并将数据同步到其他节点，需要主动维持心跳
	Dead                     // 死亡状态
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

// LogEntry 日志结构
type LogEntry struct {
	Command interface{} // 数据内容
	Term    int         // 任期
	Index   int
}

func NewLogEntry(index, term int, command interface{}) LogEntry {
	return LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
}

// ConsensusModule 共识模块结构
type ConsensusModule struct {
	mu sync.Mutex // 用于高并发中的数据安全

	total    int
	id       int            // 自身id
	peerIds  map[int]string // 其他节点的id
	LeaderId int

	server     *Server         // 服务器对象
	storage    *Storage        // 存储对象
	commitChan chan<- ApplyMsg // 用于传输已提交数据的通道

	newCommitReadyChan chan struct{} // 用于通知数据已准备好提交

	triggerAEChan chan struct{} // 用于触发同步日志操作

	currentTerm int      // 当前任期
	votedFor    int      // 投票对象
	log         *RaftLog // WAL策略中的日志

	state              CMState   // 节点状态 Candidate/Follower/Leader/Dead
	electionResetEvent time.Time // 选举超时开始计算的时间，通过更新它来保证心跳

	nextIndex  map[int]int // 下一个需要同步的日志索引
	matchIndex map[int]int // 已同步的日志索引
}

func NewConsensusModule(id int, peerIds map[int]string, server *Server, storage *Storage, ready <-chan interface{}, commitChan chan<- ApplyMsg) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.LeaderId = -1
	cm.peerIds = peerIds
	cm.total = len(peerIds) + 1
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 16)
	cm.state = Follower
	cm.votedFor = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	cm.log = NewRaftLog(storage, 1000)
	rs, err := cm.storage.LoadRuntimeState()
	if err == nil {
		cm.dlog("has data,rs=%+v", rs)
		cm.currentTerm = rs.Term
		cm.votedFor = rs.VotedFor
	} else if os.IsExist(err) {
		cm.dlog("no file,err:%v", err)
	} else {
		cm.dlog("loadRuntimeData err:%v", err)
	}

	go func() {
		// 阻塞等待其他操作准备好
		//<-ready
		cm.mu.Lock()
		// 初始化心跳事件
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		// 启动定时器
		cm.runElectionTimer()
	}()
	// 启动用于处理提交数据的协程
	go cm.commitChanSender()
	return cm
}

// Report 返回节点的基本信息
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}
func (cm *ConsensusModule) persist(snap *SnapShot) {
	rs := NewRuntimeState(cm.currentTerm, cm.votedFor, cm.log.CommitIndex)
	if len(cm.log.Entries) > 0 {
		rs.Logs = cm.log.Entries
	}
	cm.storage.Persist(rs, snap)
	cm.dlog("persist done,log=%+v", cm.log)
}

// Submit 提交数据,返回是否是leader
func (cm *ConsensusModule) Submit(command interface{}) (int, bool) {
	cm.mu.Lock()
	cm.dlog("Submit received by %v: %v", cm.state, command)
	// 只有Leader节点可以接收提交操作
	if cm.state == Leader {
		// 添加数据
		entry := NewLogEntry(cm.log.nextIndex(), cm.currentTerm, command)
		cm.log.Append(entry)
		// 持久化
		cm.persist(nil)
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		// 写入chan中通知协程提交数据给其他节点
		cm.triggerAEChan <- struct{}{}
		return entry.Index, true
	}
	cm.mu.Unlock()
	return -1, false
}

func (cm *ConsensusModule) GetLeaderId() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.LeaderId
}

// Stop 停止共识模块
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// 设置节点状态
	cm.state = Dead
	cm.dlog("becomes Dead")
	// 关闭提交准备通道，对应监听该channel的协程会被关闭
	close(cm.triggerAEChan)
	close(cm.newCommitReadyChan)
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// RequestVoteArgs 拉票请求
type RequestVoteArgs struct {
	Term         int // 请求方任期
	CandidateId  int // 候选人Id(请求方)
	LastLogIndex int // 请求方最新的日志索引
	LastLogTerm  int // 请求方最新的日志任期
}

// RequestVoteReply 拉票响应
type RequestVoteReply struct {
	Term        int // 响应方任期
	LeaderId    int
	VoteGranted bool // 是否同意投票
}

// RequestVote 处理投票逻辑
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// 节点死亡无需处理
	if cm.state == Dead {
		return nil
	}
	// 获得最新的数据状态
	lastLogIndex, lastLogTerm := cm.log.lastIndex(), cm.log.lastTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)
	// 对方任期更高，成为Follower,会更新任期，投票等信息
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term, -1)
	}

	if cm.currentTerm == args.Term && // 任期相同
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) && // 未投票或投票对象是拉票节点
		(args.LastLogTerm > lastLogTerm || // 对方任期更新，表明数据更新
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) { // 任期相同，对方日志索引更大，表示数据更新
		// 同意投票
		reply.VoteGranted = true
		// 更新投票对象
		cm.votedFor = args.CandidateId
		// 更新选举时间，此时也是收到选举的远程调度请求，所以需要更新这个选举时间
		cm.resetElectionEvent()
	} else {
		// 不满足条件，对方数据状态还没当前节点新
		// 不同意
		reply.VoteGranted = false
	}
	// 返回当前的节点的任期，如果当前节点的任期更高则可以帮助对方成为Follower
	reply.Term = cm.currentTerm
	reply.LeaderId = cm.LeaderId
	// 持久化内存中的log
	cm.persist(nil)
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

// AppendEntriesArgs 日志同步请求包
type AppendEntriesArgs struct {
	Term     int // 发送方任期
	LeaderId int // LeaderId

	PrevLogIndex int        // 上一条同步日志的索引
	PrevLogTerm  int        // 上一条同步日志的任期
	Entries      []LogEntry // 日志内容
	LeaderCommit int        // Leader已提交日志的索引
}

// AppendEntriesReply 日志同步响应包
type AppendEntriesReply struct {
	Term      int  // 响应方任期
	Success   bool // 是否成功
	LeaderId  int
	NextIndex int
}

// AppendEntries 处理同步日志的逻辑
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer func() {
		reply.Term = cm.currentTerm
		reply.LeaderId = cm.LeaderId
		cm.persist(nil)
		cm.mu.Unlock()
	}()
	// 死亡节点无需处理
	if cm.state == Dead {
		reply.NextIndex = -1
		return nil
	}
	cm.dlog("AppendEntries Args from node[%v]: %+v,currentTerm=%v,commitIndex=%v", args.LeaderId, args, cm.currentTerm, cm.log.CommitIndex)

	// 对方任期小，是无效请求
	if args.Term < cm.currentTerm {
		cm.dlog("... args is invalid,args.Term(%v) < currentTerm(%v)", args.Term, cm.currentTerm)
		// 这里可以利用返回的-1将这个回复包忽略，避免由于忽略滞后请求从而导致leader中对应nextIndex被置为0
		reply.NextIndex = -1
		return nil
	}
	// 对方任期更高，当前节点成为Follower，会初始化状态、投票等信息
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term, args.LeaderId)
	}

	reply.Success = false
	if cm.state == Candidate {
		cm.becomeFollower(args.Term, args.LeaderId)
	}
	// 后面每一处返回都需要设置reply.term,这里统一设置
	reply.Term = cm.currentTerm
	// 当前节点是收到Leader的日志同步包，所以需要更新心跳时间
	cm.resetElectionEvent()
	reply.Success = false
	lastSnapshotIndex := cm.log.lastSnapshotIndex()
	// 上次同步内容在这里已经被持久化,但是leader的日志是最新的
	// 这里存在不可纠正的错误,需要全量同步
	if args.PrevLogIndex < lastSnapshotIndex {
		// 设置下一次全量同步(从索引0开始)
		reply.NextIndex = 0
		return nil
	}
	// PrevLogIndex必定大于当前日志的开始索引，所以还需要判断是否在索引范围内
	// 不在范围内：PrevLogIndex>lastIndex，下一次同步索引取当前的lastIndex+1
	lastIndex := cm.log.lastIndex()
	if args.PrevLogIndex > lastIndex {
		reply.NextIndex = lastIndex + 1
		return nil
	}

	//cm.dlog("matchLog, args%+v,log=%+v", args, cm.log.Entries)
	// 在范围内则判断数据是否是正确的，也就是index和term是否对的上
	// 数据正确则开始同步，数据不正确则开始寻找冲突
	if !cm.log.matchLog(args.PrevLogIndex, args.PrevLogTerm) {
		// 存在数据冲突的情况
		// 从冲突位置向前寻找数据正确的索引
		// 找到定位到上一个任期,也就是跳过当前任期
		conflictTerm := cm.log.term(args.PrevLogIndex)
		var i int
		for i = args.PrevLogIndex; i >= lastSnapshotIndex; i-- {
			if cm.log.term(i) != conflictTerm {
				break
			}
		}
		reply.NextIndex = i + 1
		return nil
	}
	reply.Success = true
	//数据正确的情况
	logInsertIndex := cm.log.getRelIndex(args.PrevLogIndex + 1)
	newEntriesIndex := 0
	for {
		// 对索引范围的判断
		if logInsertIndex >= cm.log.size() || newEntriesIndex >= len(args.Entries) {
			break
		}
		if cm.log.Entries[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
			break
		}
		logInsertIndex++
		newEntriesIndex++
	}
	cm.dlog("logInsertIndex=%v,newEntriesIndex=%v,log=%+v", logInsertIndex, newEntriesIndex, cm.log.Entries)
	// 存在需要插入的数据
	if newEntriesIndex < len(args.Entries) {
		cm.dlog("... inserting entries %+v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
		// 插入数据
		cm.log.Entries = append(cm.log.Entries[:logInsertIndex], args.Entries[newEntriesIndex:]...)
		cm.dlog("... log is now: %+v", cm.log)
		reply.NextIndex = cm.log.nextIndex()
	}
	// Leader提交的进度大于当前节点
	if args.LeaderCommit > cm.log.CommitIndex {
		// 当前节点更新提交进度
		cm.log.CommitIndex = common.MinInt(args.LeaderCommit, cm.log.lastIndex())
		cm.dlog("... setting commitIndex=%d", cm.log.CommitIndex)
	}
	// 应用进度落后，通知应用数据到状态机
	if cm.log.Applied < cm.log.CommitIndex {
		// 通知提交数据
		cm.newCommitReadyChan <- struct{}{}
	}

	cm.dlog("AppendEntries reply to Node[%v]: %+v", args.LeaderId, *reply)
	return nil
}

func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	// 只有Leader才会对其他节点发送心跳包以及日志同步信息
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	cm.mu.Unlock()
	// 为每个节点开启协程发送心跳包
	for peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			lastSnapshotIndex := cm.log.lastSnapshotIndex()
			cm.dlog("Node[%d] ni:=%v,lastSnapShotIndex=%v,currentLog=%+v", peerId, ni, lastSnapshotIndex, cm.log.Entries)
			if ni <= lastSnapshotIndex {
				// 全量同步
				cm.dlog("Node[%v] need Full Sync!!!!!", peerId)
				runtime := NewRuntimeState(cm.currentTerm, cm.votedFor, cm.log.CommitIndex)
				runtime.Logs = cm.log.Entries
				snap := cm.storage.LoadSnapShot()
				args := InstallSnapshotArgs{
					Term:     cm.currentTerm,
					LeaderId: cm.id,
					Runtime:  runtime,
					Snapshot: snap,
				}
				cm.mu.Unlock()

				var reply InstallSnapshotReply
				// 获得请求后的内容
				if err := cm.server.Call(peerId, "ConsensusModule.InstallSnapShot", args, &reply); err == nil {
					cm.mu.Lock()
					defer cm.mu.Unlock()
					// 返回的任期更大，说明当前节点已经落后，成为Follower
					if reply.Term > args.Term {
						cm.dlog("term out of date in heartbeat reply")
						cm.becomeFollower(reply.Term, reply.LeaderId)
						return
					}
					// 返回的任期更小，是无效的响应包
					if reply.Term < args.Term {
						return
					}
					// 当前节点已经不是Leader了,说明这些响应包是过期无效的，不需要处理
					if cm.state != Leader {
						cm.dlog("term out of date in heartbeat reply")
						return
					}
					// 接下来的标明是正确的响应包，走响应逻辑
					cm.nextIndex[peerId] = cm.log.CommitIndex + 1
					cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
					cm.dlog("install snapshot to Node[%v] args=%+v, reply=%+v", args.LeaderId, args, reply)
				}
			} else {
				// 增量同步
				// 之前已经被接收的日志的最后一个索引
				pervLogIndex := ni - 1
				pervLogTerm := cm.log.term(pervLogIndex)
				entries := cm.log.Entries[ni:]
				// 包装成请求包
				args := AppendEntriesArgs{
					Term:         cm.currentTerm,
					LeaderId:     cm.id,
					PrevLogTerm:  pervLogTerm,
					PrevLogIndex: pervLogIndex,
					Entries:      entries,
					LeaderCommit: cm.log.CommitIndex,
				}
				cm.dlog("sending AppendEntries to %v: ni=%d,args=%+v", peerId, ni, args)
				cm.mu.Unlock()

				var reply AppendEntriesReply
				// 获得请求后的内容
				if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
					cm.mu.Lock()
					defer cm.mu.Unlock()
					// 返回的任期更大，说明当前节点已经落后，成为Follower
					if reply.Term > args.Term {
						cm.dlog("term out of date in heartbeat reply")
						cm.becomeFollower(reply.Term, reply.LeaderId)
						return
					}
					// 返回的任期更小，是无效的响应包
					if reply.Term < args.Term {
						return
					}
					// 当前节点已经不是Leader了,说明这些响应包是过期无效的，不需要处理
					if cm.state != Leader {
						cm.dlog("term out of date in heartbeat reply")
						return
					}
					// 接下来的标明是正确的响应包，走响应逻辑
					// 同步成功
					if reply.Success {
						// 更新下一个发送的索引
						cm.nextIndex[peerId] = ni + len(args.Entries)
						// 更新已经复制成功的索引
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						// 记录当前的已提交的索引
						savedCommitIndex := cm.log.CommitIndex
						// 一个个检查是否被大多数接收成功
						for i := savedCommitIndex + 1; i <= cm.log.lastIndex(); i++ {
							// 只有任期相同的数据才是有效的
							if cm.log.term(i) == args.Term {
								// 统计符合的节点个数
								matchCount := 1
								for pId := range cm.peerIds {
									// 如果已复制的日志索引大于当前判断的日志的索引则表示当前日志位置已经被这个节点接收成功
									if cm.matchIndex[pId] >= i {
										matchCount++
									}
								}
								// 大多数节点接收成功，则更新当前节点(Leader端)的已提交索引
								// 这里体现了大多数节点已经成功提交则认为提交成功
								if matchCount*2 > cm.total {
									//cm.dlog("Leader commitIndex from %v to %v", cm.log.CommitIndex, i)
									cm.log.CommitIndex = i
								}
							}
						}
						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, match := %v;commitIndex:= %d",
							peerId, cm.nextIndex[peerId], cm.matchIndex, cm.log.CommitIndex)
						// 和原先的已提交索引进行比较，若有变化则代表有日志被集群提交
						if cm.log.CommitIndex != savedCommitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.log.CommitIndex)
							// 通过通道告知已经有日志数据被提交
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						}
					} else { // 日志提交失败，需要修改发送日志的位置
						if reply.NextIndex != -1 {
							cm.nextIndex[peerId] = reply.NextIndex
						}
					}
				}

			}
		}(peerId)
	}
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int
	Runtime  *RuntimeState
	Snapshot *SnapShot
}

type InstallSnapshotReply struct {
	Term     int
	LeaderId int
}

func (cm *ConsensusModule) InstallSnapShot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	cm.mu.Lock()
	defer func() {
		reply.Term = cm.currentTerm
		reply.LeaderId = cm.LeaderId
		cm.persist(args.Snapshot)
		cm.mu.Unlock()
	}()
	// 死亡节点无需处理
	if cm.state == Dead {
		return nil
	}
	cm.dlog("InstallSnapShot from node[%v]: %+v,currentTerm=%v,commitIndex=%v", args.LeaderId, args, cm.currentTerm, cm.log.CommitIndex)

	// 对方任期小，是无效请求
	if args.Term < cm.currentTerm {
		cm.dlog("... args is invalid,args.Term(%v) < currentTerm(%v)", args.Term, cm.currentTerm)
		return nil
	}
	// 对方任期更高，当前节点成为Follower，会初始化状态、投票等信息
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term, args.LeaderId)
	}

	if cm.state == Candidate {
		cm.becomeFollower(args.Term, args.LeaderId)
	}
	// 后面每一处返回都需要设置reply.term,这里统一设置
	// 当前节点是收到Leader的日志同步包，所以需要更新心跳时间
	cm.resetElectionEvent()
	reply.LeaderId = cm.LeaderId
	snapIndex := args.Snapshot.MetaData.Index
	snapTerm := args.Snapshot.MetaData.Term
	if snapIndex <= cm.log.CommitIndex {
		cm.dlog("Nde[%v] ignored snapshot Index=%v,Term=%v", cm.id, snapIndex, snapTerm)
		return nil
	}
	cm.log.Entries = args.Runtime.Logs
	cm.log.CommitIndex = args.Runtime.CommitIndex
	cm.newCommitReadyChan <- struct{}{}
	go func() {
		cm.commitChan <- NewApplyMsgFromSnapshot(args.Snapshot)
	}()
	return nil
}

// ###########################################选举模块############################################################################
// 获得超时时间
func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// 选举定时器
// 非Leader节点会运行运行器，超时则进入选举阶段
// 超时时间是随机的，定时器会被节点状态、任期变化，超时所打断
func (cm *ConsensusModule) runElectionTimer() {
	timeDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C
		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}
		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeDuration {
			cm.dlog("heart timeout,start election,timeout=%v,elapsed=%v", timeDuration, elapsed)
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// 选举逻辑
func (cm *ConsensusModule) startElection() {
	// 改变状态为Candidate(候选人)
	cm.state = Candidate
	cm.LeaderId = -1
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	// 更新计时器时间
	cm.resetElectionEvent()
	// 投票给自己
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate(currentTerm=%d); log=%v", savedCurrentTerm, cm.log)
	// 已经有了自己的一票
	voteReceived := 1
	// 给每个节点发消息拉票
	for peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			// 获得自己的记录状态
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()
			// 包装请求
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)

			var reply RequestVoteReply
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				// 成功获得返回的处理逻辑

				cm.mu.Lock()
				cm.dlog("received proto.RequestVoteReply %+v", reply)
				// 判断当期节点是否还是Candidate，可能会被其他协程改变，如果不是则不需要走下面的逻辑
				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					cm.mu.Unlock()
					return
				}
				// 对方任期更大，表示对方数据更新，当前节点不可能当选Leader，转为Follower节点
				if reply.Term > cm.currentTerm {
					cm.dlog("term out of date in proto.RequestVoteReply")
					cm.becomeFollower(reply.Term, reply.LeaderId)
					cm.mu.Unlock()
					return
				} else if reply.Term == savedCurrentTerm { // 任期相同
					if reply.VoteGranted { // 成功拉到票
						// 票数+1
						voteReceived += 1
						// 大多数同意则成为Leader
						if voteReceived*2 > cm.total {
							cm.dlog("wins election with %d vote", voteReceived)
							cm.startLeader()
							cm.mu.Unlock()
							// 帮助集群提交
							cm.triggerAEChan <- struct{}{}
							return
						}
					}
				}
				cm.mu.Unlock()
			}
		}(peerId)
	}
	// 因为任期增加原来的timer会停止，所以需要新跑一个
	go cm.runElectionTimer()
}

// 成为Follower
func (cm *ConsensusModule) becomeFollower(term, leaderId int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	// 设置节点状态
	cm.state = Follower
	cm.LeaderId = leaderId
	// 更新任期
	cm.currentTerm = term
	// 初始状态没有投票对象
	cm.votedFor = -1
	// 更新定时器时间
	cm.resetElectionEvent()
	// 启动定选举超时定时器
	go cm.runElectionTimer()
}

// 成为Leader
func (cm *ConsensusModule) startLeader() {
	// 设置状态
	cm.state = Leader
	cm.LeaderId = cm.id
	// 初始化Leader维护的每个节点的下一个索引的位置以及当前已经复制的日志索引
	for peerId := range cm.peerIds {
		cm.nextIndex[peerId] = cm.log.nextIndex()
		cm.matchIndex[peerId] = 0
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v,log=%+v",
		cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log.Entries)

	// 开启心跳协程
	go func(heartbeatTimeout time.Duration) {
		// 协程刚开始时发送一次心跳包
		cm.leaderSendAEs()
		// 创建定时器
		t := time.NewTimer(heartbeatTimeout)
		// 方法结束时定时器也需要结束
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C: // 心跳定时器触发
				doSend = true
				// 已经读取channel中的内容所以通道中已经没有内容
				// 避免资源泄露
				t.Stop()
				// 重置定时器
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.triggerAEChan: // 有日志需要同步触发
				if ok {
					doSend = true
				} else {
					return
				}
				// 检查返回值并清空通道
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend { // 需要发送数据
				cm.mu.Lock()
				// 只有Leader可以发送心跳包
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				// 发送心跳包(日志同步包)
				cm.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)

}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log.Entries) > 0 {
		lastIndex := len(cm.log.Entries) - 1
		return lastIndex, cm.log.Entries[lastIndex].Term
	} else {
		return -1, -1
	}
}

// 数据准备好后发送数据
func (cm *ConsensusModule) commitChanSender() {
	// 监听数据Ready channel，通过Ready Channel可以得知是否有数据准备好提交
	// channel关闭时退出for循环
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		// 记录最后一位应用于状态机的索引
		savedLastApplied := cm.log.Applied
		var entries []LogEntry
		// 提交索引大于应用于状态机的索引，表明存在数据未被应用于状态机
		if cm.log.CommitIndex <= cm.log.Applied {
			cm.mu.Unlock()
			continue
		}
		// 获得这部分数据
		entries = cm.log.Entries[cm.log.Applied+1 : cm.log.CommitIndex+1]
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d,commitIndex=%v",
			entries, savedLastApplied, cm.log.CommitIndex)
		cm.mu.Unlock()

		// 将数据封装后发送给 专用于传递提交数据的通道
		for _, entry := range entries {
			cm.commitChan <- NewApplyMsgFromEntry(entry)
		}
		cm.mu.Lock()
		// 更新应用进度
		if cm.log.Applied < cm.log.CommitIndex {
			cm.log.Applied = cm.log.CommitIndex
		}
		cm.mu.Unlock()

	}
}

func (cm *ConsensusModule) resetElectionEvent() {
	elapsed := time.Since(cm.electionResetEvent)
	cm.electionResetEvent = time.Now()
	cm.dlog("heartbeat update, elapsed=%v", elapsed)
}

func (cm *ConsensusModule) PersistStateAndSnapShot(index int, data []byte) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	snapShot := cm.log.createSnapShot(index, data)
	if snapShot == nil {
		return
	}
	// 压缩日志
	cm.log.compact(snapShot.MetaData.Index)
	cm.persist(snapShot)
	return
}

func (cm *ConsensusModule) PersistSnapShot(snap *SnapShot) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if snap == nil {
		return
	}
	// 压缩日志
	cm.log.compact(snap.MetaData.Index)
	cm.persist(snap)
	return
}
