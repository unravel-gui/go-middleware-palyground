package part2

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DebugCM = 1

type CommitEntry struct {
	Command interface{}

	Index int

	Term int
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

type LogEntry struct {
	Command interface{}
	Term    int
}

type ConsensusModule struct {
	mu      sync.Mutex
	id      int
	peerIds []int
	server  *Server

	commitChan         chan<- CommitEntry
	newCommitReadyChan chan struct{}

	currentTerm int
	voteFor     int
	log         []LogEntry

	commitIndex        int // 记录已提交日志的最高索引
	lastApplied        int // 记录应用到状态机的最高索引
	state              CMState
	electionResetEvent time.Time

	nextIndex  map[int]int // 记录下一次同步日志的索引
	matchIndex map[int]int // 记录日志已经复制成功的索引
}

func NewConsensusModule(id int, peerIds []int, server *Server,
	ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.state = Follower
	cm.voteFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	// 开启协程运行选举定时器
	go func() {
		// 等待其他准备工作完成
		<-ready
		cm.mu.Lock()
		// 更新选举时间
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		// 运行选举超时定时器程序
		cm.runElectionTimer()
	}()
	// 开启接收已提交日志的协程
	go cm.commitChanSender()
	return cm
}

// Report 获得节点的基本信息
// 节点id、任期以及是否是Leader
func (cm *ConsensusModule) Report() (id, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Submit 提交数据到主节点中
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.dlog("Submit received by %v:%v", cm.state, command)
	// 只有leader节点才能提交
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.dlog("... log-%v", cm.log)
		return true
	}
	return false
}

// Stop 关闭共识模块
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// 修改节点状态为Dead
	cm.state = Dead
	cm.dlog("becomes Dead")
	// 关闭channel后监听该channel的协程也会结束
	close(cm.newCommitReadyChan)
}

// 封装一层打印函数，方便使用参数控制打印的日志内容
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	// 获得当前节点最新的数据状态(索引以及任期)
	lastLongIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, voterFor=%d, log index/term=(%d, %d)]",
		args, cm.currentTerm, cm.voteFor, lastLongIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// 根据数据状态的新旧来判断是否投票
	if cm.currentTerm == args.Term && // 任期相同的情况
		(cm.voteFor == -1 || cm.voteFor == args.CandidateId) && // 没有投票或者之前投的也是这个节点
		(args.LastLogTerm > lastLogTerm) || // 拉票的节点任期更大则代表数据更新
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLongIndex) { // 拉票节点和当前节点的任期相同，则比较日志索引，索引大于当前节点则说明数据更新
		// 拉票节点的数据更新，同意本次投票
		reply.VoteGranted = true
		cm.voteFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	// 更新任期
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int        // 上一次日志同步的索引
	PrevLogTerm  int        // 上一次日志同步的任期
	Entries      []LogEntry // 日志
	LeaderCommit int        //
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries 同步日志
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)
	// 对方的任期更大则自己成为Follower
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		// 对方发送同步日志，表明对象是Leader
		// 那么判断当前节点状态，如果不是Follower则改为Follower
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		// 更新选举时间
		cm.electionResetEvent = time.Now()
		if args.PrevLogIndex == -1 || // 日志为空
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			// 上一次同步的日志索引应当比当前日志小:
			// 从PrevLogIndex位置往后的都是脏数据
			// 两者的任期必须相同
			// 更新同步结果
			reply.Success = true
			// 同步日志开始位置
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			// 找到插入的位置
			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// 提交的位置小于数据长度，则插入数据
			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}
			// leader的提交索引大于当前节点的提交索引
			if args.LeaderCommit > cm.commitIndex {
				// 复制为leader节点和当前日志长度的最小值
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		}

	}
	// 更新返回的任期
	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStart := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStart)

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

		if cm.currentTerm != termStart {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStart, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.voteFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	voteReceived := 1

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.dlog("while wating for reply, state = %v", cm.state)
					return
				}
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						voteReceived += 1
						if voteReceived*2 > len(cm.peerIds)+1 {
							cm.dlog("wins election with %d votes", voteReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.voteFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader

	for _, peerId := range cm.peerIds {
		// 更新为当前节点的日志长度
		// 新成为leader，目前日志都是已经提交的
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			cm.leaderSendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	if cm.state == Dead {
		cm.mu.Unlock()
		return
	}
	// 当前任期
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			// 下一次同步的位置
			ni := cm.nextIndex[peerId]
			// 日志下一次同步的索引
			pervLogIndex := ni - 1
			// 日志下一次同步的任期
			pervLogTerm := -1
			if pervLogIndex >= 0 {
				pervLogTerm = cm.log[pervLogIndex].Term
			}
			// 需要同步的日志内容
			entries := cm.log[ni:]
			// 构造日志同步请求
			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: pervLogIndex,
				PrevLogTerm:  pervLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex, // 提交的索引
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > cm.currentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
				// 只有Leader节点并且任期没有发生变才能走提交的逻辑
				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						// 同步成功
						// 更新记录下一次同步的索引位置
						cm.nextIndex[peerId] = ni + len(entries)
						// 更新复制成功的索引
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						cm.dlog("AppendEntries reply from %d success: nextIndex :=%+v, matchIndex:= %v", peerId, cm.nextIndex, cm.matchIndex)
						// 记录当前提交成功的索引
						savedCommitIndex := cm.commitIndex
						// 遍历待提交部分
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								// 日志的任期与当前节点的任期相同
								matchCount := 1
								// 计算当前位置的已经复制成功的节点数量
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								// 大多数已经复制成功则更新提交的索引
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						// 判断是否有日志提交成功
						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader sets commitIndex:= %d", cm.commitIndex)
							// 有数据提交成功，发送提交准备信息
							cm.newCommitReadyChan <- struct{}{}
						}
					} else {
						// 同步失败则更新下一次发送的日志索引
						cm.nextIndex[peerId] = ni - 1
						cm.dlog("AppendEntries reply from %d success: nextIndex:= %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// 将发送已经提交的数据
func (cm *ConsensusModule) commitChanSender() {
	// 循环监听提交channel
	for range cm.newCommitReadyChan {
		// 有日志提交时
		cm.mu.Lock()
		// 记录当前任期
		savedTerm := cm.currentTerm
		// 记录当前最新的应用于状态机的日志索引
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		// 提交索引大于应用与状态机的数据代表时需要提交的
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			// 将数据发送到channel中
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1, // 提交数据的index
				Term:    savedTerm,
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
