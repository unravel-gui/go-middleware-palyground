package part6

import (
	"fmt"
	"math/rand"
	"os"
	"raft/part5/common"
	logger "raft/part6/log"
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

// ConsensusModule 共识模块结构
type ConsensusModule struct {
	mu sync.Mutex // 用于高并发中的数据安全
	n  int

	id       int64 // 自身id
	endpoint string
	peers    map[int64]*RaftPeer // 其他节点的id

	leaderId  int64
	server    *Server         // 服务器对象
	storage   *Persister      // 存储对象
	applyChan chan<- ApplyMsg // 用于传输已提交数据的通道

	newApplyReadyChan chan struct{} // 用于通知数据已准备好提交

	triggerAEChan chan struct{} // 用于触发同步日志操作

	currentTerm int64    // 当前任期
	votedFor    int64    // 投票对象
	logs        *RaftLog // WAL策略中的日志

	state              CMState   // 节点状态 Candidate/Follower/Leader/Dead
	electionResetEvent time.Time // 选举超时开始计算的时间，通过更新它来保证心跳

	nextIndex  map[int64]int64 // 下一个需要同步的日志索引
	matchIndex map[int64]int64 // 已同步的日志索引

	dlog logger.Logger
}

func NewConsensusModule(id int64, endpoint string, server *Server,
	storage *Persister, ready <-chan interface{}, applyChan chan<- ApplyMsg, log logger.Logger) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.endpoint = endpoint
	cm.server = server
	cm.storage = storage
	cm.n = len(server.peerIds) + 1
	cm.dlog = log
	cm.applyChan = applyChan
	cm.newApplyReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.logs = NewRaftLog(storage, 1000)
	cm.nextIndex = make(map[int64]int64)
	cm.matchIndex = make(map[int64]int64)

	rs := cm.storage.LoadRuntimeState()
	if rs != nil {
		cm.currentTerm = rs.Term
		cm.votedFor = rs.VotedFor
	}

	go func() {
		fmt.Printf("NewConsensusModule000000000000\n")
		defer fmt.Printf("NewConsensusModule1111111111111\n")
		// 阻塞等待其他操作准备好
		<-ready
		cm.mu.Lock()
		// 初始化
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.newPeers(cm.server.peerIds)
		// 启动定时器
		cm.runElectionTimer()
	}()
	// 启动用于处理提交数据的协程
	go cm.applyChanSender()
	return cm
}

func (cm *ConsensusModule) newPeers(servers map[int64]string) {
	cm.peers = make(map[int64]*RaftPeer)
	for i, server := range servers {
		cm.AddRaftPeer(i, server)
	}
}
func (cm *ConsensusModule) AddRaftPeer(id int64, endpoint string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, exists := cm.peers[id]; exists {
		cm.dlog.Warn("id=%v is existed, raftPeer=%+v", id, cm.peers[id])
		return false
	}
	var err error
	cm.peers[id], err = NewRaftPeer(id, endpoint)
	if err != nil {
		cm.dlog.Error("addRaftPeer err:%v", err)
		return false
	}
	cm.nextIndex[id] = 0
	cm.matchIndex[id] = 0
	cm.dlog.Info("addRaftPeer [id=%v, endpoint=%v] success", id, endpoint)
	return true
}

func (cm *ConsensusModule) RemoveRaftPeer(id int64) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if peer, exists := cm.peers[id]; exists && peer != nil {
		if peer.rpcClient != nil {
			peer.rpcClient.Close()
			peer.rpcClient = nil
		}
		delete(cm.peers, id)
		cm.dlog.Info("Node [%v] is removed", id)
	}
	return true
}

// Report 返回节点的基本信息
func (cm *ConsensusModule) Report() (id int64, term int64, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Submit 提交数据
func (cm *ConsensusModule) Submit(data []byte) bool {
	cm.mu.Lock()
	cm.dlog.Info("Submit received by %v: %v", cm.state, string(data))
	// 只有Leader节点可以接收提交操作
	if cm.state == Leader {
		entry := LogEntry{
			Term:  cm.currentTerm,
			Index: cm.logs.lastIndex() + 1,
			Data:  data}
		// 添加数据
		cm.logs.Append(entry)
		cm.dlog.Info("... log=%v", cm.logs.String())
		cm.triggerAEChan <- struct{}{}
		cm.mu.Unlock()
		// 写入chan中通知协程提交数据给其他节点
		return true
	}

	cm.mu.Unlock()
	return false
}

// Stop 停止共识模块
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// 设置节点状态
	cm.state = Dead
	cm.dlog.Info("becomes Dead")
	// 关闭提交准备通道，对应监听该channel的协程会被关闭
	close(cm.newApplyReadyChan)
	close(cm.triggerAEChan)
}

// RequestVote 处理投票逻辑
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer func() {
		cm.persist(nil)
		cm.dlog.Info("[RequestVote reply] to Node[%v]: reply:=%+v, args=%v,state=%v",
			args.CandidateId, *reply, args, cm.state)
		cm.mu.Unlock()
	}()
	// 节点死亡无需处理
	if cm.state == Dead {
		return nil
	}
	// 获得最新的数据状态
	//lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog.Debug("RequestVote: %+v [currentTerm=%d, votedFor=%d, raftLog=%+v]",
		args, cm.currentTerm, cm.votedFor, cm.logs)
	// 对方任期更高，成为Follower,会更新任期，投票等信息
	if args.Term > cm.currentTerm {
		//cm.dlog.Info("... term out of date in RequestVote")
		cm.becomeFollower(args.Term, cm.leaderId)
	}
	// 默认为false
	reply.VoteGranted = false
	if cm.currentTerm == args.Term && // 任期相同
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) && // 未投票或投票对象是拉票节点
		(cm.logs.IsUpToDate(args.LastLogIndex, args.LastLogTerm)) { // 根据数据状态判断是否同意投票
		// 同意投票
		reply.VoteGranted = true
		// 更新投票对象
		cm.votedFor = args.CandidateId
		// 更新选举时间，此时也是收到选举的远程调度请求，所以需要更新这个选举时间
		cm.resetElectionEvent()
	}
	reply.Term = cm.currentTerm
	reply.LeaderId = cm.leaderId
	return nil
}

// AppendEntriesArgs 日志同步请求包
//type AppendEntriesArgs struct {
//	Term     int // 发送方任期
//	LeaderId int // LeaderId
//
//	PrevLogIndex int        // 上一条同步日志的索引
//	PrevLogTerm  int        // 上一条同步日志的任期
//	Entries      []LogEntry // 日志内容
//	LeaderCommit int        // Leader已提交日志的索引
//}
//
//// AppendEntriesReply 日志同步响应包
//type AppendEntriesReply struct {
//	Term    int  // 响应方任期
//	Success bool // 是否成功
//
//	ConflictIndex int // 同步出现问题的索引
//	ConflictTerm  int // 同步出现问题的任期
//}

// AppendEntries 处理同步日志的逻辑
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer func() {
		cm.persist(nil)
		cm.dlog.Info("[AppendEntries reply] to Node[%v]: reply:=%+v, args=%v", args.LeaderId, *reply, args)
		cm.mu.Unlock()
	}()
	// 死亡节点无需处理
	if cm.state == Dead {
		return nil
	}
	//cm.dlog.Info("before appendEntries AppendEntriesArgs: %+v", args)

	// 对方任期更高，当前节点成为Follower，会初始化状态、投票等信息
	if args.Term > cm.currentTerm ||
		(args.Term == cm.currentTerm && cm.state == Candidate) {
		cm.dlog.Info("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term, args.LeaderId)
	}

	cm.resetElectionEvent()
	reply.Success = false
	// 给的数据增量同步数据中间缺失一段
	if args.PrevLogIndex < cm.logs.lastSnapshotIndex() {
		reply.Term = 0
		reply.LeaderId = -1
		cm.dlog.Debug("receives unexpected AppendEntriesArgs %v from Node[%v] "+
			"because prevLogIndex %v < firstLogIndex %v",
			args, args.LeaderId, args.PrevLogIndex, cm.logs.firstIndex())
		return nil
	}
	// 尝试添加日志
	lastIndex := cm.logs.maybeAppend(args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	cm.dlog.Debug("receives from Node[%v] AppendEntriesArgs %v, lastIndex=%v",
		args.LeaderId, args, lastIndex)
	if lastIndex < 0 {
		// 存在冲突
		conflictIndex := cm.logs.FindConflict(args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = cm.currentTerm
		reply.LeaderId = cm.leaderId
		reply.Success = false
		reply.NextIndex = conflictIndex
		return nil
	}
	reply.Term = cm.currentTerm
	reply.LeaderId = cm.leaderId
	reply.Success = true
	reply.NextIndex = lastIndex + 1
	return nil
}

// 获得超时时间
func (cm *ConsensusModule) electionTimeout() time.Duration {
	electionBaseTime := config.RaftConfig.ElectionBaseTime
	electionRandomTime := config.RaftConfig.ElectionRandomTime
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(electionBaseTime) * time.Millisecond
	} else {
		return time.Duration(electionBaseTime+rand.Intn(electionRandomTime)) * time.Millisecond
	}
}

// 选举定时器
// 非Leader节点会运行运行器，超时则进入选举阶段
// 超时时间是随机的，定时器会被节点状态、任期变化，超时所打断
func (cm *ConsensusModule) runElectionTimer() {
	fmt.Printf("runElectionTimer000000000000\n")
	defer fmt.Printf("runElectionTimer1111111111111\n")
	timeDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.dlog.Info("election timer started (%v), term=%d,currentState=%v", timeDuration, termStarted, cm.state.String())
	cm.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C
		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog.Info("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}
		if termStarted != cm.currentTerm {
			cm.dlog.Info("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeDuration {
			cm.dlog.Info("in election timer start election in term=%v,timeout=%s,elapsed=%s bailing out",
				cm.currentTerm, timeDuration.String(), elapsed.String())
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
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	// 更新计时器时间
	cm.resetElectionEvent()
	// 投票给自己
	cm.votedFor = cm.id
	cm.dlog.Info("becomes Candidate(currentTerm=%d); term=%v", savedCurrentTerm, cm.currentTerm)

	args := RequestVoteArgs{
		Term:         savedCurrentTerm,
		CandidateId:  cm.id,
		LastLogIndex: cm.logs.lastIndex(),
		LastLogTerm:  cm.logs.lastTerm(),
	}
	// 已经有了自己的一票
	voteReceived := 1
	// 给每个节点发消息拉票
	for id, peer := range cm.peers {
		if peer == nil {
			cm.dlog.Error("Node[%d] RaftPeer is nil", id)
			continue
		}
		go func(peer *RaftPeer) {
			fmt.Printf("startElection000000000000\n")
			defer fmt.Printf("startElection1111111111111\n")
			cm.dlog.Debug("sending RequestVote to %d: %+v", peer.id, args)
			var reply RequestVoteReply
			if err := peer.requestVote(args, &reply); err == nil {
				// 成功获得返回的处理逻辑
				cm.mu.Lock()
				defer cm.mu.Unlock()
				// 判断当期节点是否还是Candidate，可能会被其他协程改变，如果不是则不需要走下面的逻辑
				if cm.state != Candidate {
					//cm.dlog.Info("while waiting for reply, state = %v", cm.state)
					return
				}
				// 对方任期更大，表示对方数据更新，当前节点不可能当选Leader，转为Follower节点
				if reply.Term > cm.currentTerm {
					cm.dlog.Info("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term, reply.LeaderId)
					return
				} else if reply.Term == savedCurrentTerm { // 任期相同
					cm.dlog.Info("received from Node [%v] RequestVoteReply %+v", peer.id, reply)
					if reply.VoteGranted { // 成功拉到票
						// 票数+1
						voteReceived += 1
						// 大多数同意则成为Leader
						if voteReceived*2 > cm.n {
							cm.dlog.Info("wins election with %d vote", voteReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peer)
	}
	// 因为任期增加原来的timer会停止，所以需要新跑一个
	go cm.runElectionTimer()
}

// 成为Follower
func (cm *ConsensusModule) becomeFollower(term int64, leaderId int64) {
	cm.dlog.Info("becomes Follower with term=%d; leaderId=%v", term, cm.leaderId)
	// 设置节点状态
	cm.state = Follower
	cm.leaderId = -1
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
	cm.leaderId = cm.id
	// 初始化Leader维护的每个节点的下一个索引的位置以及当前已经复制的日志索引
	for peerId, _ := range cm.server.peerIds {
		cm.nextIndex[peerId] = cm.logs.lastIndex() + 1
		cm.matchIndex[peerId] = 0
	}
	cm.dlog.Info("becomes Leader; term=%d", cm.currentTerm)
	heartbeatTimeout := config.RaftConfig.HeartbeatTimeout
	if heartbeatTimeout == 0 {
		heartbeatTimeout = common.RAFT_HEARTNBEAT_TIMEOUT
	}
	timeout := time.Duration(heartbeatTimeout) * time.Millisecond
	// 开启心跳协程
	go func(heartbeatTimeout time.Duration) {
		fmt.Printf("startLeader000000000000\n")
		defer fmt.Printf("startLeader1111111111111\n") // 协程刚开始时发送一次心跳包
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
	}(timeout)
}

// 给集群中每个节点发送心跳包(日志同步包)
func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	// 只有Leader才会对其他节点发送心跳包以及日志同步信息
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	// 记录刚开始的任期
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()
	// 为每个节点开启协程发送心跳包
	for id, peer := range cm.peers {
		if peer == nil {
			cm.dlog.Error("Node[%d] RaftPeer is nil", id)
			continue
		}
		go func(peer *RaftPeer) {
			fmt.Printf("leaderSendAEs000000000000\n")
			defer fmt.Printf("leaderSendAEs1111111111111\n")
			cm.mu.Lock()
			prevIndex := cm.nextIndex[peer.id] - 1
			// 进度落后于当前持久化进度，直接发送快照
			if prevIndex < cm.logs.lastSnapshotIndex() {

			} else {
				entries := cm.logs.GetEntries(cm.nextIndex[peer.id])
				args := AppendEntriesArgs{
					Term:         cm.currentTerm,
					LeaderId:     cm.id,
					PrevLogTerm:  cm.logs.term(prevIndex),
					PrevLogIndex: prevIndex,
					Entries:      entries,
					LeaderCommit: cm.logs.CommitIndex, // 当前leader的已经提交的索引
				}
				cm.dlog.Info("sending AppendEntries to %v: ni=%d,args=%+v", peer.id, cm.nextIndex[peer.id], args)
				cm.mu.Unlock()

				var reply AppendEntriesReply
				// 获得请求后的内容
				if err := peer.appendEntries(args, &reply); err == nil {
					cm.mu.Lock()
					defer cm.mu.Unlock()
					// 返回的任期更大，说明当前节点已经落后，成为Follower
					if reply.Term > savedCurrentTerm {
						cm.dlog.Info("term out of date in heartbeat reply")
						cm.becomeFollower(reply.Term, reply.LeaderId)
						return
					}
					// 过期的响应
					if cm.state != Leader || reply.Term < cm.currentTerm {
						return
					}
					// 同步失败，更新nextIndex即可，冲突位置已被找出
					if !reply.Success {
						if reply.NextIndex > 0 {
							cm.nextIndex[peer.id] = reply.NextIndex
							cm.matchIndex[peer.id] = common.MinInt64(cm.matchIndex[peer.id], reply.NextIndex-1)
						}
						return
					}
					if reply.NextIndex > cm.nextIndex[peer.id] {
						cm.nextIndex[peer.id] = reply.NextIndex
						cm.matchIndex[peer.id] = reply.NextIndex - 1
					}
					lastIndex := cm.matchIndex[peer.id]
					matchCount := 1
					// 一个个检查是否被大多数接收成功
					for _, index := range cm.matchIndex {
						// 只有任期相同的数据才是有效的
						if index >= lastIndex {
							matchCount++
						}
						if matchCount*2 > cm.n {
							if cm.logs.maybeCommit(lastIndex, cm.currentTerm) {
								cm.newApplyReadyChan <- struct{}{}
							}
						}
					}
				}
			}
		}(peer)
	}
}

// 数据准备好后发送数据
func (cm *ConsensusModule) applyChanSender() {
	fmt.Printf("applyChanSender000000000000\n")
	defer fmt.Printf("applyChanSender1111111111111\n")
	// 监听数据Ready channel，通过Ready Channel可以得知是否有数据准备好提交
	// channel关闭时退出for循环
	for range cm.newApplyReadyChan {
		cm.mu.Lock()
		lastCommitIndex := cm.logs.CommitIndex
		// 获得需要发送的命令
		entries := cm.logs.NextEntries()
		// 封装要发送的命令
		msgs := make([]ApplyMsg, 0)
		for _, entry := range entries {
			msgs = append(msgs, NewApplyMsgFromEntry(entry))
		}
		cm.mu.Unlock()

		for _, msg := range msgs {
			cm.applyChan <- msg
		}
		cm.mu.Lock()
		cm.dlog.Info("applyChanSender entries=%v, in term=%d", entries, cm.currentTerm)
		cm.logs.AppliedTo(common.MaxInt64(cm.logs.Applied, lastCommitIndex))
		cm.mu.Unlock()
	}
	cm.dlog.Info("applyChanSender done")
}

func (cm *ConsensusModule) persist(snap *SnapShot) {
	rs := NewRuntimeState(cm.currentTerm, cm.votedFor, cm.logs.CommitIndex)
	cm.storage.Persist(rs, cm.logs.Entries, snap)
}

func (cm *ConsensusModule) resetElectionEvent() {
	cm.electionResetEvent = time.Now()
}
