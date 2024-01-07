package part3

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
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
}

// CommitEntry 提交数据的结构
type CommitEntry struct {
	Command interface{} // 数据内容
	Index   int         // 日志中对应的索引
	Term    int         // 任期
}

// ConsensusModule 共识模块结构
type ConsensusModule struct {
	mu sync.Mutex // 用于高并发中的数据安全

	id      int   // 自身id
	peerIds []int // 其他节点的id

	server     *Server            // 服务器对象
	storage    Storage            // 存储对象
	commitChan chan<- CommitEntry // 用于传输已提交数据的通道

	newCommitReadyChan chan struct{} // 用于通知数据已准备好提交

	triggerAEChan chan struct{} // 用于触发同步日志操作

	currentTerm int        // 当前任期
	votedFor    int        // 投票对象
	log         []LogEntry // WAL策略中的日志

	commitIndex        int       // 已提交的日志索引
	lastApplied        int       // 已经用于状态机的日志索引
	state              CMState   // 节点状态 Candidate/Follower/Leader/Dead
	electionResetEvent time.Time // 选举超时开始计算的时间，通过更新它来保证心跳

	nextIndex  map[int]int // 下一个需要同步的日志索引
	matchIndex map[int]int // 已同步的日志索引
}

func NewConsensusModule(id int, peerIds []int, server *Server, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	if cm.storage.HasData() {
		cm.restoreFromStorage()
	}

	go func() {
		// 阻塞等待其他操作准备好
		<-ready
		cm.mu.Lock()
		// 更新心跳时间
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

// Submit 提交数据
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	cm.dlog("Submit received by %v: %v", cm.state, command)
	// 只有Leader节点可以接收提交操作
	if cm.state == Leader {
		// 添加数据
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		// 持久化
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		// 写入chan中通知协程提交数据给其他节点
		cm.triggerAEChan <- struct{}{}
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
	cm.dlog("becomes Dead")
	// 关闭提交准备通道，对应监听该channel的协程会被关闭
	close(cm.newCommitReadyChan)
}

// 从存储中加载数据,包含currentTerm、voteFor、log
func (cm *ConsensusModule) restoreFromStorage() {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewReader(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if voteData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(voteData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

// 持久化内存数据,包含currentTerm、voteFor、log
func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var voteData bytes.Buffer
	if err := gob.NewEncoder(&voteData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", voteData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
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
	Term        int  // 响应方任期
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
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)
	// 对方任期更高，成为Follower,会更新任期，投票等信息
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
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
		cm.electionResetEvent = time.Now()
	} else {
		// 不满足条件，对方数据状态还没当前节点新
		// 不同意
		reply.VoteGranted = false
	}
	// 返回当前的节点的任期，如果当前节点的任期更高则可以帮助对方成为Follower
	reply.Term = cm.currentTerm
	// 持久化内存中的log
	cm.persistToStorage()
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
	Term    int  // 响应方任期
	Success bool // 是否成功

	ConflictIndex int // 同步出现问题的索引
	ConflictTerm  int // 同步出现问题的任期
}

// AppendEntries 处理同步日志的逻辑
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// 死亡节点无需处理
	if cm.state == Dead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	// 对方任期更高，当前节点成为Follower，会初始化状态、投票等信息
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm { // 任期相同
		if cm.state != Follower { // 若不是Follower则成为Follower
			cm.becomeFollower(args.Term)
		}
		// 当前节点是收到Leader的日志同步包，所以需要更新心跳时间
		cm.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 || // 之前没有同步过
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			// 之前同步的索引存在并且任期信息符合
			// 同步信息没有问题，标记为true
			reply.Success = true
			// 双指针找到两边同步开始的位置
			// 数据将从这个位置开始插入log
			logInsertIndex := args.PrevLogIndex + 1
			// 收到的日志数据将从这个位置开始拷贝
			newEntriesIndex := 0
			// 找到需要同步日志的两个索引的位置
			for {
				// 开始插入的索引不可能>=日志的长度
				// 待接收数据的索引也不可能>=待接收数据的长度
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				// 数据的任期不同，后面的是脏数据，需要抛弃
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// 存在需要插入的数据
			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				// 插入数据
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}
			// Leader提交的进度大于当前节点
			if args.LeaderCommit > cm.commitIndex {
				// 当前节点更新提交进度
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				// 存在已经准备好提交的数据,通过channel告知另一个协程进行操作
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			// 上一次提交的日志索引比当前日志数据的长度都要大
			// 说明中间存在数据丢失
			if args.PrevLogIndex >= len(cm.log) {
				// 丢失数据索引开始的位置
				reply.ConflictIndex = len(cm.log)
				// 数据丢失，与任期无关
				reply.ConflictTerm = -1
			} else { // 数据未丢失，但出现错误
				//  数据不符合
				// 出现问题的任期
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term

				var i int
				// 任期相同的数据是没有问题的，所以从后向前寻找出现冲突任期的位置
				// reply.ConflictTerm是正常数据的最后一个任期，从这个任期往后就是脏数据
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				// 记录出现冲突任期的第一个索引位置，也就是脏数据的第一位
				reply.ConflictIndex = i + 1
			}
		}
	}
	// 更新任期
	reply.Term = cm.currentTerm
	// 提交日志后持久化一次
	cm.persistToStorage()
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

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
	cm.electionResetEvent = time.Now()
	// 投票给自己
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate(currentTerm=%d); log=%v", savedCurrentTerm, cm.log)
	// 已经有了自己的一票
	voteReceived := 1
	// 给每个节点发消息拉票
	for _, peerId := range cm.peerIds {
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
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)
				// 判断当期节点是否还是Candidate，可能会被其他协程改变，如果不是则不需要走下面的逻辑
				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					return
				}
				// 对方任期更大，表示对方数据更新，当前节点不可能当选Leader，转为Follower节点
				if reply.Term > cm.currentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm { // 任期相同
					if reply.VoteGranted { // 成功拉到票
						// 票数+1
						voteReceived += 1
						// 大多数同意则成为Leader
						if voteReceived*2 > len(cm.peerIds)+1 {
							cm.dlog("wins election with %d vote", voteReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}
	// 因为任期增加原来的timer会停止，所以需要新跑一个
	go cm.runElectionTimer()
}

// 成为Follower
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	// 设置节点状态
	cm.state = Follower
	// 更新任期
	cm.currentTerm = term
	// 初始状态没有投票对象
	cm.votedFor = -1
	// 更新定时器时间
	cm.electionResetEvent = time.Now()
	// 启动定选举超时定时器
	go cm.runElectionTimer()
}

// 成为Leader
func (cm *ConsensusModule) startLeader() {
	// 设置状态
	cm.state = Leader
	// 初始化Leader维护的每个节点的下一个索引的位置以及当前已经复制的日志索引
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

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
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			// 获得节点下一个等待发送的索引
			ni := cm.nextIndex[peerId]
			// 之前已经被接收的日志的最后一个索引
			pervLogIndex := ni - 1
			// 之前已经被接收的日志的最后一个任期
			pervLogTerm := -1
			if pervLogIndex >= 0 {
				pervLogTerm = cm.log[pervLogIndex].Term
			}
			// 获得在那之后的日志数据
			entries := cm.log[ni:]
			// 包装成请求包
			args := AppendEntriesArgs{
				Term:         cm.currentTerm,
				LeaderId:     cm.id,
				PrevLogTerm:  pervLogTerm,
				PrevLogIndex: pervLogIndex,
				Entries:      entries,
				LeaderCommit: cm.commitIndex, // 当前leader的已经提交的索引
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d,args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			// 获得请求后的内容
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				// 返回的任期更大，说明当前节点已经落后，成为Follower
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
				// 当前节点是Leader并且任期相同
				if cm.state == Leader && savedCurrentTerm == reply.Term {
					// 同步成功
					if reply.Success {
						// 更新下一个发送的索引
						cm.nextIndex[peerId] = ni + len(entries)
						// 更新已经复制成功的索引
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						// 记录当前的已提交的索引
						savedCommitIndex := cm.commitIndex
						// 一个个检查是否被大多数接收成功
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							// 只有任期相同的数据才是有效的
							if cm.log[i].Term == cm.currentTerm {
								// 统计符合的节点个数
								matchCount := 1
								for _, peerId := range cm.peerIds {
									// 如果已复制的日志索引大于当前判断的日志的索引则表示当前日志位置已经被这个节点接收成功
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								// 大多数节点接收成功，则更新当前节点(Leader端)的已提交索引
								// 这里体现了大多数节点已经成功提交则认为提交成功
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, match := %v;commitIndex:= %d", peerId, cm.nextIndex[peerId], cm.matchIndex, cm.commitIndex)
						// 和原先的已提交索引进行比较，若有变化则代表有日志被集群提交
						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
							// 通过通道告知已经有日志数据被提交
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						}
					} else { // 日志提交失败，需要修改发送日志的位置
						// 出现冲突的任期大于等于0
						// 表示数据存在冲突
						if reply.ConflictTerm >= 0 {
							// 记录这个出现冲突的索引
							lastIndexOfTerm := -1
							// 找出这个冲突任期的最后一条日志数据的索引
							// 这里的冲突任期实际是正常数据的最后一个任期，这个任期之后的数据是有冲突的
							for i := len(cm.log) - 1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							// 冲突索引存在
							if lastIndexOfTerm >= 0 {
								// +1后即为冲突的数据的第一位索引
								cm.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								// 更新为 记录的问题索引
								cm.nextIndex[peerId] = reply.ConflictIndex
							}
						} else { // 存在数据丢失的情况
							// 更新为 记录的问题索引
							cm.nextIndex[peerId] = reply.ConflictIndex
						}
						cm.dlog("AppendEntries reply from %d success:nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

// 获得最近日志数据的索引和任期
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
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
		// 记录当前任期
		savedTerm := cm.currentTerm
		// 记录最后一位应用于状态机的索引
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		// 提交索引大于应用于状态机的索引，表明存在数据未被应用于状态机
		if cm.commitIndex > cm.lastApplied {
			// 获得这部分数据
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			// 更新索引
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)
		// 将数据封装后发送给 专用于传递提交数据的通道
		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,            // 数据本体
				Index:   savedLastApplied + i + 1, // 在日志中的索引
				Term:    savedTerm,                // 当前任期
			}
		}
	}
	cm.dlog("commitChanSender done")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
