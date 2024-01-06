package part1

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DebugCM = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
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

// ConsensusModule 共识模块提供数据一致性的服务
type ConsensusModule struct {
	mu sync.Mutex

	id int // 当前id

	peerIds []int // 其他节点id

	server *Server // raft服务器对象

	currentTerm int        // 当前term
	votedFor    int        // 为谁投票
	log         []LogEntry //日志内容

	state              CMState   // 状态
	electionResetEvent time.Time // 选举重置时间，记录上一次选举的时间
}

// NewConsensusModule 新建共识模块对象
// 共识需要的参数以及启动选取计时器
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower // 一开始默认是Follower
	cm.votedFor = -1
	go func() {
		<-ready // 等待准备完毕
		cm.mu.Lock()
		cm.electionResetEvent = time.Now() // 更新选举时间
		cm.mu.Unlock()
		cm.runElectionTimer() // 开启选举监听器
	}()
	return cm
}

// Report 返回当前id，任期和是否是leader
func (cm *ConsensusModule) Report() (id, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Stop 停止当前节点
// 修改当前节点状态为死亡
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("becomes Dead")
}

// 封装一层用于打印的函数
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d]", cm.id) + format
		log.Printf(format, args...)
	}
}

// RequestVoteArgs 拉票请求
type RequestVoteArgs struct {
	Term        int // 当前任期
	CandidateId int // 当前候选人id，也就是投票对象
	// 数据信息用于判断数据新旧
	LastLogIndex int //	最后一次的日志索引
	LastLogTerm  int // 最后一次的任期	重要程度 Term>Index
}

// RequestVoteReply 拉票响应
type RequestVoteReply struct {
	Term        int  // 拉票对象的任期，用于判断节点数据状态
	VoteGranted bool //是否同意投票
}

// RequestVote 处理拉票请求函数
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		// 节点已死则不参与投票
		return nil
	}
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		// 对方的任期更高，说明对方的状态更新，那么当前节点不可能成为新Leader，默默成为Follower就行
		cm.dlog("... term out of date in RequestVote")
		// 成为Follower
		cm.becomeFollower(args.Term)
	}
	if cm.currentTerm == args.Term && // 任期相同表明两者新旧状态一致，那么只需要考虑是否投票给他就行
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) { //-1 代表还没投票，相同则表示之前也是投票这个拉票对象
		reply.VoteGranted = true           // 统一投票
		cm.votedFor = args.CandidateId     // 记录投票对象
		cm.electionResetEvent = time.Now() // 更新选举时间
	} else {
		reply.VoteGranted = false //	不投票，可能的情况：当前任期比他大，或者自己已经投了其他节点的票
	}
	reply.Term = cm.currentTerm // 返回自己的任期
	cm.dlog("... RequestVote reply:%+v", reply)
	return nil
}

// AppendEntriesArgs 同步日志请求
type AppendEntriesArgs struct {
	Term     int // 当前任期
	LeaderId int // leaderId
	// 用于增量同步
	PrevLogIndex int        // 上一次同步的日志索引
	PrevLogTerm  int        // 上移同步的日志任期  Term>Index
	Entries      []LogEntry // 日志的实体对象
	LeaderCommit int        // 头节点是否提交
}

// AppendEntriesReply 日志同步响应
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries 处理同步日志请求
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		// 死亡节点继续潜水即可
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		// 自己的任期更低，安安心心当Follower
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		// 任期相同则需要进行同步操作，如果自己任期更高则说明自己的数据更新
		if cm.state != Follower {
			// 如果不是Follower那么转换角色(状态)
			cm.becomeFollower(args.Term)
		}
		// 更新选举时间
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}
	// 返回自己的任期
	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

// 获得选举超时
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// 增加随机性
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 { // 开启强制更多连任选项后加一个1/3概率
		// 获得更短的选举超时时间
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// 启动选举超时计时器
func (cm *ConsensusModule) runElectionTimer() {
	// 获得选举超时时间
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	// 记录开始的任期
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)
	// 创建计时器对象
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop() // 函数结束前停止计时器
	for {
		<-ticker.C // 等待计时器给信号
		cm.mu.Lock()
		// 只有Candidate和Follower需要启动选举超时计时器
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}
		// 因为是定时任务所以状态可能已经发生改变
		// 如果不是原来的周期则停止计时
		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}
		// 判断是否超时
		// electionResetEvent会由其他操作进行更新
		// 这里根据这个重置的选举时间开始到现在来判断是否超过之前定下的随机超时时间
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			// 超时则开启新leader的选举
			// 里面会异步去拉票
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// 开始新leader的选举
func (cm *ConsensusModule) startElection() {
	// 当前节点开启选举状态为Candidate(候选人)
	// 这部分的信息的多线程数据安全由调用函数保证
	cm.state = Candidate
	// 进入新的任期
	cm.currentTerm += 1
	// 保存当前任期
	savedCurrentTerm := cm.currentTerm
	// 更新重置选举时间
	cm.electionResetEvent = time.Now()
	// 投票给自己
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)
	// 已经有自己的一票
	votesReceived := 1

	// 遍历所有peer去拉票
	for _, peerId := range cm.peerIds {
		// 使用协程去异步执行
		go func(peerId int) {
			// 拉票请求
			args := RequestVoteArgs{
				Term:        savedCurrentTerm, // 自己的任期
				CandidateId: cm.id,            // 自己的id
			}
			var reply RequestVoteReply

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			// 远程调用获得拉票结果
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)
				// 如果自己已经不是Candidate则不需要在走后面的投票统计
				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					return
				}
				// 返回的任期更高，表示对方的数据更新，自己成为Follower即可
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm { // 任期没有发生变化
					// 变化的原因:
					//		如果其他协程获得更高的term那么本节点的term就会因为becomeFollower函数更新term
					// 		又触发了超时选举，任期加一

					// 成功拉到票
					if reply.VoteGranted {
						// 票数+1
						votesReceived += 1
						// 大多数同意投票
						if votesReceived*2 > len(cm.peerIds)+1 {
							cm.dlog("wins election with %d votes", votesReceived)
							// 成为leader
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}
	// 启动选举超时计时器
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	// 开启心跳检测机制
	go cm.runElectionTimer()
}

// 成为leader
func (cm *ConsensusModule) startLeader() {
	// 这部分数据安全由调用函数保证
	// 更新状态为Leader
	cm.state = Leader
	cm.dlog("becomes Leader; term=%d, log=%+v", cm.currentTerm, cm.log)

	// 开启协程用于维护心跳
	go func() {
		// 获得计时器
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		// 定时任务：发送心跳
		for {
			// 发送心跳
			cm.leaderSendHeartbeats()
			// 等待计时器
			<-ticker.C
			cm.mu.Lock()
			// 如果当前节点已经不是leader那么停止发送心跳
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

// leader发送心跳函数
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	// 如果不是Leader则不需要发送心跳
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	// 记录当前任期
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()
	// 遍历使用客户端发送心跳包和日志同步包 (其实心跳内容被蕴含在日志包中)
	for _, peerId := range cm.peerIds {
		// 创建同步日志请求对象
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}
		// 开启协程处理发送
		go func(peerId int) {
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%v", peerId, 0, args)
			var reply AppendEntriesReply
			// 远程调用发送同步日志包
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				// 如果对方任期更高则说明自己数据已经旧了，那么成为Follower即可
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}
