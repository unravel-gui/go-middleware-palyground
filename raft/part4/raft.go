package part4

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
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

	newCommitReadyChan   chan struct{} // 用于通知数据已准备好提交
	persistenceReadyChan chan struct{} // 用于触发持久化
	triggerAEChan        chan struct{} // 用于触发同步日志操作

	currentTerm int        // 当前任期
	votedFor    int        // 投票对象
	log         []LogEntry // WAL策略中的日志

	commitIndex        int       // 已提交的日志索引
	lastApplied        int       // 已经用于状态机的日志索引
	state              CMState   // 节点状态 Candidate/Follower/Leader/Dead
	electionResetEvent time.Time // 选举超时开始计算的时间，通过更新它来保证心跳

	nextIndex  map[int]int // 下一个需要同步的日志索引
	matchIndex map[int]int // 已同步的日志索引

	commandMap   map[int]int    // 保存数据
	ac           *AtomicCounter // 触发快照
	persistIndex int            // 标记持久化的日志索引
	lastLogTerm  int            // 记录最新日志的term
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
	cm.persistIndex = -1
	cm.persistenceReadyChan = make(chan struct{}, 1)
	cm.commandMap = make(map[int]int)
	cm.ac = NewAtomicCounter(5)
	cm.lastLogTerm = -1

	if cm.storage.HasData() {
		cm.dlog("has data")
		cm.restoreFromStorage()
		cm.dlog("restart from File:currentTerm=%+v,persistIndex=%+v,log=%+v,commandMap=%+v,lastTotalLogTerm=%+v",
			cm.currentTerm, cm.persistIndex, cm.log, cm.commandMap, cm.lastLogTerm)
	} else {
		cm.dlog("start CM without data")
	}

	go func() {
		// 阻塞等待其他操作准备好
		<-ready
		cm.mu.Lock()
		// 更新心跳时间
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		// 启动定时器
		go cm.runElectionTimer()
		// 启用持久化
		persistTimeout := time.Duration(150) * time.Millisecond
		go cm.persistCommitLogTimer(persistTimeout)
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

// Submit 将修改操作写入日志
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	cm.dlog("Submit received by %v: %v", cm.state, command)
	// 只有Leader节点可以接收提交操作
	if cm.state == Leader {
		// 添加数据
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.lastLogTerm = cm.currentTerm
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
	close(cm.persistenceReadyChan)
	close(cm.triggerAEChan)
	cm.dlog("close channel of 2 ready and 1 trigger")
}

// 从存储中加载数据,包含currentTerm、voteFor、log
func (cm *ConsensusModule) restoreFromStorage() {

	// 先恢复数据快照
	if commandData := cm.storage.getSnapShot(); commandData != nil {
		d := gob.NewDecoder(bytes.NewBuffer(commandData))
		if err := d.Decode(&cm.commandMap); err != nil {
			log.Fatalf("restroe running data, decode `commandMap` err=%+v", err)
		}
		cm.dlog("running data load success")
	} else {
		// 文件为空，那么加载运行时数据成功后将丢失数据
		// 所以直接返回，类似新增一个新的节点
		log.Printf("commandMap not found in storage")
		return
	}

	if exist := cm.storage.restoreFromFile(); exist {
		if termData, found := cm.storage.Get("currentTerm"); found {
			d := gob.NewDecoder(bytes.NewReader(termData))
			if err := d.Decode(&cm.currentTerm); err != nil {
				log.Fatalf("restroe running data, decode `currentTerm` err=%+v", err)
			}
		} else {
			log.Printf("currentTerm not found in storage")
		}
		if voteData, found := cm.storage.Get("votedFor"); found {
			d := gob.NewDecoder(bytes.NewBuffer(voteData))
			if err := d.Decode(&cm.votedFor); err != nil {
				log.Fatalf("restroe running data, decode `votedFor` err=%+v", err)
			}
		} else {
			log.Printf("votedFor not found in storage")
		}
		if logData, found := cm.storage.Get("log"); found {
			d := gob.NewDecoder(bytes.NewBuffer(logData))
			if err := d.Decode(&cm.log); err != nil {
				log.Fatalf("restroe running data, decode `log` err=%+v", err)
			}
		} else {
			log.Printf("log not found in storage")
		}

		if acData, found := cm.storage.Get("ac"); found {
			d := gob.NewDecoder(bytes.NewBuffer(acData))
			var value int64
			if err := d.Decode(&value); err != nil {
				log.Fatalf("restroe running data, decode `ac` err=%+v", err)
			}
			cm.ac.SetValue(value)
		} else {
			log.Printf("ac not found in storage")
		}
		if persistIndexData, found := cm.storage.Get("persistIndex"); found {
			d := gob.NewDecoder(bytes.NewBuffer(persistIndexData))
			if err := d.Decode(&cm.persistIndex); err != nil {
				log.Fatalf("restroe running data, decode `persistIndex` err=%+v", err)
			}
			cm.commitIndex = cm.persistIndex
		} else {
			log.Printf("persistIndex not found in storage")
		}
		if lastLogTermData, found := cm.storage.Get("lastLogTerm"); found {
			d := gob.NewDecoder(bytes.NewBuffer(lastLogTermData))
			if err := d.Decode(&cm.lastLogTerm); err != nil {
				log.Fatalf("restroe running data, decode `lastLogTerm` err=%+v", err)
			}
		} else {
			log.Printf("lastLogTerm not found in storage")
		}
		// 存在持久化进度，由日志同步功能进行同步
		if lastAppliedData, found := cm.storage.Get("lastApplied"); found {
			d := gob.NewDecoder(bytes.NewBuffer(lastAppliedData))
			if err := d.Decode(&cm.lastApplied); err != nil {
				log.Fatalf("restroe running data, decode `lastApplied` err=%+v", err)
			}
		} else {
			log.Printf("lastApplied not found in storage")
		}

		cm.dlog("running data load success")
	}

}

// 持久化内存数据,包含currentTerm、voteFor、log
// 由外部调用的函数获得锁
func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatalf("persis running data, encode `currentTerm` err=%+v", err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var voteData bytes.Buffer
	if err := gob.NewEncoder(&voteData).Encode(cm.votedFor); err != nil {
		log.Fatalf("persis running data, encode `votedFor` err=%+v", err)
	}
	cm.storage.Set("votedFor", voteData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatalf("persis running data, encode `log` err=%+v", err)
	}
	cm.storage.Set("log", logData.Bytes())

	var acData bytes.Buffer
	if err := gob.NewEncoder(&acData).Encode(cm.ac.Value()); err != nil {
		log.Fatalf("persis running data, encode `ac` err=%+v", err)
	}
	cm.storage.Set("ac", acData.Bytes())

	var persistIndexData bytes.Buffer
	if err := gob.NewEncoder(&persistIndexData).Encode(cm.persistIndex); err != nil {
		log.Fatalf("persis running data, encode `persistIndex` err=%+v", err)
	}
	cm.storage.Set("persistIndex", persistIndexData.Bytes())

	var lastLogTermData bytes.Buffer
	if err := gob.NewEncoder(&lastLogTermData).Encode(cm.lastLogTerm); err != nil {
		log.Fatalf("persis running data, encode `lastLogTerm` err=%+v", err)
	}
	cm.storage.Set("lastLogTerm", lastLogTermData.Bytes())

	var lastAppliedData bytes.Buffer
	if err := gob.NewEncoder(&lastAppliedData).Encode(cm.lastApplied); err != nil {
		log.Fatalf("persis running data, encode `lastApplied` err=%+v", err)
	}
	cm.storage.Set("lastApplied", lastAppliedData.Bytes())
	// 开协程去落盘
	go func() {
		cm.storage.PersistToFile()
	}()
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// RequestVoteArgs 拉票请求
type RequestVoteArgs struct {
	Term              int // 请求方任期
	CandidateId       int // 候选人Id(请求方)
	LastTotalLogIndex int // 请求方最新的日志索引
	LastLogTerm       int // 请求方最新的日志任期
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
	// 计算最新的数据状态
	// 获得日志中的数据
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	// 算上已抛弃日志的最新索引，当前日志的索引可能不对应但是加上已抛弃的日志必须要要对应
	lastTotalLogIndex := cm.getTotalLogIndex(lastLogIndex)
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastTotalLogIndex, lastLogTerm)
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term && // 任期相同
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) && // 未投票或投票对象是拉票节点
		(args.LastLogTerm > lastLogTerm || // 对方任期更新，表明数据更新，
			(args.LastLogTerm == lastLogTerm && args.LastTotalLogIndex >= lastTotalLogIndex)) { // 任期相同，对方日志索引更大，表示数据更新
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

	PrevTotalLogIndex int        // 上一条同步日志的索引
	PrevLogTerm       int        // 上一条同步日志的任期
	Entries           []LogEntry // 日志内容
	LeaderTotalCommit int        // Leader已提交日志的索引
	SnapShot          []byte
	Ac                int
	LastApplied       int
	PersistIndex      int
	LastLogTerm       int
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
	//cm.dlog("AppendEntries: %+v, commitIndex=%v", args, cm.commitIndex)
	// 对方任期更高，当前节点成为Follower，会初始化状态、投票等信息
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}
	reply.Success = false

	if args.Ac != -1 {
		cm.dlog("Full AppendEntries from [%+v], args=%+v", args.LeaderId, args)
		cm.FullReplication(args, reply)
		return nil
	}
	// 增量复制
	cm.IncrementalReplication(args, reply)
	return nil
}

func (cm *ConsensusModule) IncrementalReplication(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// 实现增量复制的逻辑
	if args.Term == cm.currentTerm && args.LeaderTotalCommit >= cm.commitIndex { // 任期相同并且，提交进度更新，避免旧数据包干扰
		if cm.state != Follower { // 若不是Follower则成为Follower
			cm.becomeFollower(args.Term)
		}
		// 当前节点是收到Leader的日志同步包，所以需要更新心跳时间
		cm.electionResetEvent = time.Now()
		// 当前Follower节点的总日志长度
		totalLogLength := cm.getTotalLogIndex(len(cm.log))
		// 上一次同步的绝对索引
		currentPreLogIndex := cm.getCurrentLogIndex(args.PrevTotalLogIndex)
		cm.dlog("Args for Append Logs from [%v]: [args=%+v, totalLogLength=%v,currentPreLogIndex=%v,log=%+v, persistIndex=%v,lastLogTerm=%v]",
			args.LeaderId, args, totalLogLength, currentPreLogIndex, cm.log, cm.persistIndex, cm.lastLogTerm)

		if (args.PrevTotalLogIndex == -1 && args.LeaderTotalCommit == -1) || // 第一次同步
			currentPreLogIndex == -1 || // 相对索引为-1，对应Follower节点持久化后第一次同步的情况
			(args.PrevTotalLogIndex < totalLogLength && args.LastLogTerm == cm.lastLogTerm && args.PersistIndex+len(args.Entries)+1 == totalLogLength) || // PreLogTerm=-1说明这部分日志在Leader中已经被丢弃，这里只需要判断是否是提交请求即可,
			(args.PrevTotalLogIndex < totalLogLength && // 总日志层面上一次同步的索引存在
				currentPreLogIndex >= 0 && currentPreLogIndex < len(cm.log) && // 同步位置在日志列表中
				cm.log[currentPreLogIndex].Term == args.PrevLogTerm) { // 日志列表中任期相同，数据完全没有问题
			// 之前同步的索引存在并且任期信息符合
			// 同步信息没有问题，标记为true
			reply.Success = true
			// 双指针找到两边同步开始的位置
			// 数据将从这个位置开始插入log
			logInsertIndex := currentPreLogIndex + 1
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
				cm.lastLogTerm = args.LastLogTerm
				cm.dlog("... log is now: %v", cm.log)
			}
			cm.dlog("AppendEntries Commit from [%v]: LeaderCommit=%+v, FollowerCommit=%+v,commandMap=%v", args.LeaderId, args.LeaderTotalCommit, cm.commitIndex, cm.commandMap)
			// Leader提交的进度大于当前节点
			if args.LeaderTotalCommit > cm.commitIndex {
				// 当前节点更新提交进度
				newTotalLogLength := cm.getTotalLogIndex(len(cm.log) - 1)
				savedCommitIndex := cm.commitIndex
				// 取总提交进度和当前Follower节点总日志长度的较小者
				cm.commitIndex = min(args.LeaderTotalCommit, newTotalLogLength)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				// 存在已经准备好提交的数据,通过channel告知另一个协程进行操作
				cm.newCommitReadyChan <- struct{}{}
				cm.dealWithCommands(savedCommitIndex)
				cm.dlog("Follower Commit [after] to [%v]: LeaderCommit=%+v, FollowerCommit=%+v,commandMap=%v", args.LeaderId, args.LeaderTotalCommit, cm.commitIndex, cm.commandMap)
			}
		} else {
			// 只有Leader节点和Follower节点双方都可查的数据才可以被增量修改成功，反之直接全量复制
			// 判断是否全量复制
			if args.PrevTotalLogIndex >= totalLogLength || // 总日志中上一次提交的绝对索引比当前总日志长度都要大肯定存在数据丢失
				currentPreLogIndex < 0 || currentPreLogIndex >= len(cm.log) || (args.PrevLogTerm == -1 && args.LastLogTerm != cm.lastLogTerm) { // 计算出当前日志中对应的索引不在相对日志的范围内,代表当前节点丢失持久化部分的数据，直接全量同步同步
				// 丢失数据索引开始的位置,绝对索引
				//reply.ConflictIndex = currentPreLogIndex
				reply.ConflictIndex = cm.persistIndex + 1
				// 数据丢失，与任期无关
				reply.ConflictTerm = -1
			} else { // 接收方可以增量复制，计算增量复制需要的信息
				// 这里的情况是任期不符合的情况，即存在数据冲突
				// 出现问题的任期
				reply.ConflictTerm = cm.log[currentPreLogIndex].Term
				//if cm.log[currentPreLogIndex].Term == -1 {
				//	reply.ConflictIndex = cm.persistIndex + 1
				//} else {
				var i int
				// 从后向前找到问题任期的第一个位置
				// 这里由于我们只有将提交成功的日志进行持久化，所以已经持久化的日志必定是正确的，唯一存在的数据丢失问题则由全量复制解决
				for i = currentPreLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				// 记录出现冲突任期的第一个索引位置，也就是脏数据的第一位
				// 使用绝对索引
				reply.ConflictIndex = cm.getTotalLogIndex(i + 1)
				//}
			}
		}
	}
	// 更新任期
	reply.Term = cm.currentTerm
	// 提交日志后持久化一次
	cm.persistToStorage()
	cm.dlog("AppendEntries reply to [%v]: %+v", args.LeaderId, *reply)
}

func (cm *ConsensusModule) FullReplication(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// 实现全量复制的逻辑
	if args.Term == cm.currentTerm { // 任期相同
		if cm.state != Follower { // 若不是Follower则成为Follower
			cm.becomeFollower(args.Term)
		}
		// 当前节点是收到Leader的日志同步包，所以需要更新心跳时间
		cm.electionResetEvent = time.Now()

		cm.dlog("Args for Full Append Logs to [%v]: [args=%+v, commitIndex=%v,log=%+v, persistIndex=%v]",
			args.LeaderId, args, cm.commitIndex, cm.log, cm.persistIndex)

		// 读取快照数据
		d := gob.NewDecoder(bytes.NewBuffer(args.SnapShot))
		if err := d.Decode(&cm.commandMap); err != nil && err != io.EOF {
			log.Fatalf("full sync load snapShot err=%+sv", err)
		}
		// 更新运行时数据
		cm.persistIndex = args.PersistIndex
		cm.lastApplied = args.PersistIndex
		cm.lastLogTerm = args.LastLogTerm
		cm.ac.SetValue(int64(args.Ac))
		cm.log = args.Entries
		cm.dlog("... Full inserting entries log=%+v", cm.log)
		cm.dlog("AppendEntries Full Commit to [%v]: LeaderCommit=%+v, FollowerCommit=%+v", args.LeaderId, args.LeaderTotalCommit, cm.commitIndex)
		cm.commitIndex = args.LeaderTotalCommit
		// 存在部分提交数据未被持久化，导致map中的数据并不是最新的,重新执行一遍未持久化但是已提交的命令
		if cm.persistIndex < cm.commitIndex {
			cm.dealWithCommands(cm.persistIndex)
		}
		cm.dlog("... setting commitIndex=%d", cm.commitIndex)
		reply.Success = true
		cm.dlog("[After Full Sync] currentMap=%+v", cm.commandMap)
	}
	// 更新任期
	reply.Term = cm.currentTerm
	// 提交日志后持久化一次
	cm.persistToStorage()
	cm.dlog("AppendEntries reply to [%v]: %+v", args.LeaderId, *reply)
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
				Term:              savedCurrentTerm,
				CandidateId:       cm.id,
				LastTotalLogIndex: cm.getTotalLogIndex(savedLastLogIndex), // 总日志索引
				LastLogTerm:       savedLastLogTerm,
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
		cm.nextIndex[peerId] = cm.getTotalLogIndex(len(cm.log))
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v,log=%+v, persistIndex=%+v, commitIndex=%+v, currentMap=%+v",
		cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log, cm.persistIndex, cm.commitIndex, cm.commandMap)

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

func (cm *ConsensusModule) persistCommitLogTimer(persistTimeout time.Duration) {
	// 创建定时器
	t := time.NewTimer(persistTimeout)
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
			t.Reset(persistTimeout)
		case _, ok := <-cm.persistenceReadyChan: // 有日志需要同步触发
			if ok {
				doSend = true
			} else {
				return
			}
			// 检查返回值并清空通道
			if !t.Stop() {
				<-t.C
			}
			t.Reset(persistTimeout)
		}

		if doSend { // 需要持久化
			//cm.mu.Lock()
			//// 只有leader和follower才可以持久化
			//if cm.state != Leader && cm.state != Follower {
			//	cm.mu.Unlock()
			//	return
			//}
			//cm.dlog("[persistLog] execute persist in term := %+v, state=%+v", cm.currentTerm, cm.state)
			//cm.mu.Unlock()
			// 执行持久化逻辑
			cm.PersistenceMap()
		}
	}
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
		if cm.server.peerClients[peerId] == nil {
			cm.dlog("losing connection with %+v", peerId)
			continue
		}
		go func(peerId int) {
			cm.mu.Lock()
			// 获得节点下一个等待发送的索引,nextIndex以及matchIndex中使用的是同日志的索引
			ni := cm.nextIndex[peerId]
			// 之前已经被接收的日志的最后一个索引
			pervTotalLogIndex := ni - 1
			pervLogIndex := cm.getCurrentLogIndex(pervTotalLogIndex)
			// 之前已经被接收的日志的最后一个任期
			pervLogTerm := -1
			ac := -1
			var snapShot []byte
			var entries []LogEntry
			// 日志中无保存数据，全量复制
			if pervLogIndex < -1 {
				snapShot = cm.storage.getSnapShot()
				ac = int(cm.ac.Value())
				entries = cm.log
			} else {
				if pervLogIndex != -1 {
					pervLogTerm = cm.log[pervLogIndex].Term
				}
				// 上一次发送的日志不在相对日志中,需要全量复制
				entries = cm.log[pervLogIndex+1:]
			}

			cm.dlog("sending AppendEntries to [before]%v: ni=%d,persistIndex=%v,pervLogIndex=%v", peerId, ni, cm.persistIndex, pervLogIndex)
			// 获得在那之后的日志数据
			// 包装成请求包
			args := AppendEntriesArgs{
				Term:              cm.currentTerm,
				LeaderId:          cm.id,
				PrevLogTerm:       pervLogTerm,
				PrevTotalLogIndex: pervTotalLogIndex,
				Entries:           entries,
				LeaderTotalCommit: cm.commitIndex, // 当前leader的已经提交的索引
				SnapShot:          snapShot,
				Ac:                ac,
				PersistIndex:      cm.persistIndex,
				LastLogTerm:       cm.lastLogTerm,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d,args=%+v,pervLogIndex=%v", peerId, ni, args, pervLogIndex)

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
						if ac != -1 { // 同步
							cm.nextIndex[peerId] = cm.commitIndex + 1
							cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						} else {
							// 更新下一个发送的索引
							cm.nextIndex[peerId] = ni + len(entries)
							// 更新已经复制成功的索引
							cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						}

						// 记录当前的已提交的绝对索引
						savedTotalCommitIndex := cm.commitIndex
						// 相对索引
						savedCommitIndex := cm.getCurrentLogIndex(cm.commitIndex)
						cm.dlog("[Commit Info]: check from %d to %d, savedCommitIndex=%+v,persistIndex=%+v,ni=%v,nextIndex=%v,log=%+v",
							savedCommitIndex+1, len(cm.log), savedCommitIndex, cm.persistIndex, ni, cm.nextIndex[peerId], cm.log)
						// 一个个检查是否被大多数接收成功
						for i := savedCommitIndex + 1; i < len(cm.log); i++ {
							// 只有任期相同的数据才是有效的
							if cm.log[i].Term == cm.currentTerm {
								// 统计符合的节点个数
								matchCount := 1
								for _, peerId := range cm.peerIds {
									// 如果已复制的日志索引大于当前判断的日志的索引则表示当前日志位置已经被这个节点接收成功
									// 使用绝对索引比较
									if cm.matchIndex[peerId] >= cm.getTotalLogIndex(i) {
										matchCount++
									}
								}
								// 大多数节点接收成功，则更新当前节点(Leader端)的已提交索引
								// 这里体现了大多数节点已经成功提交则认为提交成功
								if matchCount*2 > len(cm.peerIds)+1 {
									// 这里修改只可能发生一次，下一次重复逻辑会因为commitIndex发生变化儿无法进行for循环
									savedNewCommitIndex := cm.commitIndex
									cm.dlog("cpp commitIndex,outer=%v,inner=%v", savedTotalCommitIndex, savedNewCommitIndex)
									cm.commitIndex = cm.getTotalLogIndex(i)
									cm.dealWithCommands(savedNewCommitIndex)
									cm.dlog("Leader CommitIndex Update,commitIndex from %v to %v,commandMap=%+v", savedNewCommitIndex, cm.commitIndex, cm.commandMap)
								}
							}
						}
						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, match := %v;commitIndex:= %d", peerId, cm.nextIndex[peerId], cm.matchIndex[peerId], cm.commitIndex)
						// 和原先的已提交索引进行比较，若有大多数Follower复制节点成功Leader修改commitIndex，则表示数据允许被提交
						if cm.commitIndex != savedTotalCommitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
							// 通过通道告知已经有日志数据被提交
							cm.newCommitReadyChan <- struct{}{}
							// 提交成功则修改Map中的数据,计数器加一
							// 同步本次提交操作
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
						} else { // 全量复制
							cm.nextIndex[peerId] = reply.ConflictIndex
						}
						cm.dlog("AppendEntries reply from %d failed: ni=%v,nextIndex := %d, reply=[ConflictTerm=%+v,ConflictIndex=%+v]", peerId, ni, cm.nextIndex[peerId], reply.ConflictTerm, reply.ConflictIndex)
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
		return -1, cm.lastLogTerm
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
		cm.dlog("commandSender [before]: commitIndex=%v,lastApplied=%v,persistIndex=%v", cm.commitIndex, cm.lastApplied, cm.persistIndex)
		if cm.commitIndex > cm.lastApplied {
			// 获得这部分数据
			currentLasApplied := cm.getCurrentLogIndex(cm.lastApplied)
			currentCommitIndex := cm.getCurrentLogIndex(cm.commitIndex)
			entries = cm.log[currentLasApplied+1 : currentCommitIndex+1]
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
			//// 更新map
			//logEntre := entries[i]
			//command := cm.parseLogToCommand(logEntre)
			//cm.commandMap[command] = command
		}
		//if cm.ac.AddAndIsTrigger(int64(len(entries))) {
		//	cm.persistenceReadyChan <- struct{}{}
		//}
	}
	cm.dlog("commitChanSender done")
}

func (cm *ConsensusModule) PersistenceMap() {
	// 触发持久化
	// 上锁获得此刻的内存数据
	cm.mu.Lock()
	if cm.state != Leader && cm.state != Follower {
		cm.dlog("in persist timer state=%s, bailing out", cm.state)
		cm.mu.Unlock()
		return
	}

	// 应用于状态的数据已经被持久化则无需持久化
	// 还有数据为被应用于状态机则先处理这一部分数据
	if cm.lastApplied < cm.commitIndex {
		savedTerm := cm.currentTerm
		savedLastApplied := cm.getCurrentLogIndex(cm.lastApplied)
		currentCommit := cm.getCurrentLogIndex(cm.commitIndex)
		entries := cm.log[savedLastApplied+1 : currentCommit]
		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,          // 数据本体
				Index:   cm.lastApplied + i + 1, // 在日志中的索引
				Term:    savedTerm,              // 当前任期
			}
		}
		cm.lastApplied = cm.commitIndex
	}

	if cm.persistIndex == cm.commitIndex {
		cm.mu.Unlock()
		return
	}
	// 计算新需要丢弃的日志索引
	// map中保存集群中已提交的数据
	// map被持久化后意味着已经被提交的日志可以被丢弃,如需要节点需要日志恢复则直接全量同步
	// 计算日志中的相对索引
	cm.dlog("persist Info [before]: lastApplied=%+v,commitIndex=%+v,persistIndex=%+v,log==%+v", cm.lastApplied, cm.commitIndex, cm.persistIndex, cm.log)
	currentCommitIndex := cm.getCurrentLogIndex(cm.commitIndex)

	if cm.state == Follower && (cm.commandMap == nil || len(cm.commandMap) == 0) {
		cm.dlog("state=%+v, commandMap without data, persist PASS", cm.state)
		cm.mu.Unlock()
		return
	}
	// 获得map中的数据
	var commandData bytes.Buffer
	if err := gob.NewEncoder(&commandData).Encode(cm.commandMap); err != nil {
		log.Fatalf("persist map encode err=%+v", err)
	}
	// 获得完内存数据后释放锁，其他协程可以继续处理业务
	cm.dlog("state=%+v,currentTerm=%+v,persistIndex=%v, commandMap=%v,commandData=%+v", cm.state, cm.currentTerm, cm.persistIndex, cm.commandMap, commandData.Bytes())
	cm.mu.Unlock()
	// 更新快照
	cm.storage.SnapShot(commandData.Bytes())
	// 更新数据
	cm.mu.Lock()
	// 计算新的持久化索引
	cm.persistIndex = cm.commitIndex
	cm.log = cm.log[currentCommitIndex+1:]
	// 存储运行中的数据
	cm.persistToStorage()
	cm.dlog("persist Info [after]: lastApplied=%+v,commitIndex=%+v,persistIndex=%+v,log==%+v", cm.lastApplied, cm.commitIndex, cm.persistIndex, cm.log)
	cm.mu.Unlock()
	cm.dlog("PersistenceMap done")
}

func (cm *ConsensusModule) getTotalLogIndex(logIndex int) int {
	return logIndex + cm.persistIndex + 1
}
func (cm *ConsensusModule) getCurrentLogIndex(logIndex int) int {
	return logIndex - cm.persistIndex - 1
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Leader提交数据时调用
// 调用该函数时外部需要加锁
func (cm *ConsensusModule) dealWithCommand() {
	currentCommitIndex := cm.getCurrentLogIndex(cm.commitIndex)
	logEntre := cm.log[currentCommitIndex]
	command := cm.parseLogToCommand(logEntre)
	cm.commandMap[command] = command
	// 提交max条数据触发持久化
	if cm.ac.IncrementAndIsTrigger() {
		cm.persistenceReadyChan <- struct{}{}
	}
}

// 日志同步时调用
func (cm *ConsensusModule) dealWithCommands(saveCommitIndex int) {
	currentSavedCommitIndex := cm.getCurrentLogIndex(saveCommitIndex)
	currentCommitIndex := cm.getCurrentLogIndex(cm.commitIndex)
	for i := currentSavedCommitIndex + 1; i <= currentCommitIndex; i++ {
		logEntre := cm.log[i]
		command := cm.parseLogToCommand(logEntre)
		log.Printf("execute command=%v", command)
		cm.commandMap[command] = command
	}
	// 提交max条数据触发持久化
	if cm.ac.AddAndIsTrigger(int64(cm.commitIndex - saveCommitIndex)) {
		cm.persistenceReadyChan <- struct{}{}
	}
	cm.dlog("ac = %+v", cm.ac.value)
}

// 这里保存的只是一个int
func (cm *ConsensusModule) parseLogToCommand(entry LogEntry) int {
	command := entry.Command.(int)
	return command
}
