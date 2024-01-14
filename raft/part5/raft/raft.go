package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"os"
	"raft/part5/common"
	logger "raft/part5/log"
	"sync"
	"time"
)

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
		panic("Unknown CMState")
	}
}

var dlog = logger.DLogger

type LogEntry struct {
	Command interface{}
	Term    int
}

type ConsensusModule struct {
	mu sync.Mutex

	endpoint      string
	peerEndpoints []string

	server *Server // Raft服务器对象

	// 保证集群工作的运行时数据
	currentTerm        int        //当前任期
	votedFor           string     // 投票对象
	raftLog            []LogEntry // 日志对象，存储命令以及任期等信息，用于保证数据一致性
	state              CMState    // 节点状态
	commitIndex        int        // 提交进度 (绝对索引)
	persistIndex       int        // 持久化进度 (绝对索引)
	lastLogTerm        int        // 记录最新一条日志的Term
	electionResetEvent time.Time  // 上一个时间的时间

	FileMap *FileMetadataStore // 存储内存中的元数据
	//ListMap map[string]interface{} // 存储节点数据

	// 日志同步
	nextIndex     map[string]int // 下一次同步的起始索引	(绝对索引)
	matchIndex    map[string]int // 复制进度 (绝对索引)
	triggerAEChan chan struct{}
	// 持久化
	storage          Storage               // 存储对象实现 Storage接口，提供持久化服务
	persistReadyChan chan struct{}         // 用于通知持久化
	ac               *common.AtomicCounter // 计数器，用于触发持久化
}

func NewConsensusModule(endpoint string, peerEndpoints []string, server *Server, storage Storage, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.endpoint = endpoint
	cm.peerEndpoints = peerEndpoints
	cm.server = server
	cm.votedFor = ""
	cm.state = Follower
	cm.commitIndex = -1
	cm.persistIndex = -1
	cm.lastLogTerm = -1

	cm.FileMap = NewFileMetadataStore()
	//cm.ListMap = make(map[string]interface{})

	cm.nextIndex = make(map[string]int)
	cm.matchIndex = make(map[string]int)
	cm.triggerAEChan = make(chan struct{}, 1)

	cm.storage = storage
	cm.persistReadyChan = make(chan struct{}, 1)
	cm.ac = common.NewAtomicCounter(int64(cm.server.config.AcMax))

	if cm.storage.HasData() {
		dlog.Info("NewConsensusModule has data")
		// 从文件中恢复运行时数据
		cm.recoverRuntimeData()
		dlog.Info("recover from file: currentTerm=%v,persistIndex=%v,raftLog=%v,FileMap=%v,lastLogTerm=%v",
			cm.currentTerm, cm.persistIndex, cm.raftLog, cm.FileMap, cm.lastLogTerm)
	} else {
		dlog.Warn("NewConsensusModule start without data")
	}

	go func() {
		// 等待准备工作完成
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		// TODO: 启动定时器任务：心跳和持久化
	}()

	return cm
}

func (cm *ConsensusModule) Report() (endpoint string, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.endpoint, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	dlog.Info("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.raftLog = append(cm.raftLog, LogEntry{
			Command: command,
			Term:    cm.currentTerm,
		})
		cm.lastLogTerm = cm.currentTerm
		// 持久化运行时数据
		cm.persisRuntime()
		dlog.Info("... raftLog=%v", cm.raftLog)
		cm.mu.Unlock()
		// 通知日志同步
		cm.triggerAEChan <- struct{}{}
		return true
	}
	cm.mu.Unlock()
	return false
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	dlog.Info("becomes Dead")
	close(cm.triggerAEChan)
	close(cm.persistReadyChan)
}

// ##################################   持久化模块  #######################################################################

// 从文件中恢复数据，包括运行时数据和数据实体
func (cm *ConsensusModule) recoverRuntimeData() {
	// 先恢复快照数据
	if snapShot := cm.storage.getSnapShot(); snapShot != nil {
		if fileMapData, found := snapShot["FileMap"]; found {
			d := gob.NewDecoder(bytes.NewReader(fileMapData))
			if err := d.Decode(&cm.FileMap); err != nil {
				dlog.Error("recover FileMap err: %v", err)
			}
		} else {
			dlog.Error("FileMap not found in storage")
		}
	} else {
		dlog.Warn("snapShot not found")
		// 数据文件为空那么只加载运行时数据将丢失
		return
	}

	// 加载运行时数据
	if exist := cm.storage.recoverFromFile(); exist {
		if termData, found := cm.storage.Get("currentTerm"); found {
			d := gob.NewDecoder(bytes.NewReader(termData))
			if err := d.Decode(&cm.currentTerm); err != nil {
				dlog.Error("recover runtime data, decode `currentTerm` err=%+v", err)
			}
		} else {
			dlog.Error("currentTerm not found in storage")
		}
		if voteData, found := cm.storage.Get("votedFor"); found {
			d := gob.NewDecoder(bytes.NewBuffer(voteData))
			if err := d.Decode(&cm.votedFor); err != nil {
				dlog.Error("recover runtime data, decode `votedFor` err=%+v", err)
			}
		} else {
			dlog.Error("votedFor not found in storage")
		}
		if raftLogData, found := cm.storage.Get("raftLog"); found {
			d := gob.NewDecoder(bytes.NewBuffer(raftLogData))
			if err := d.Decode(&cm.raftLog); err != nil {
				dlog.Error("recover runtime data, decode `raftLog` err=%+v", err)
			}
		} else {
			dlog.Error("raftLog not found in storage")
		}

		if acData, found := cm.storage.Get("ac"); found {
			d := gob.NewDecoder(bytes.NewBuffer(acData))
			var value int64
			if err := d.Decode(&value); err != nil {
				dlog.Error("recover runtime data, decode `ac` err=%+v", err)
			}
			cm.ac.SetValue(value)
		} else {
			dlog.Error("ac not found in storage")
		}
		if persistIndexData, found := cm.storage.Get("persistIndex"); found {
			d := gob.NewDecoder(bytes.NewBuffer(persistIndexData))
			if err := d.Decode(&cm.persistIndex); err != nil {
				dlog.Error("recover runtime data, decode `persistIndex` err=%+v", err)
			}
			cm.commitIndex = cm.persistIndex
		} else {
			dlog.Error("persistIndex not found in storage")
		}
		if lastLogTermData, found := cm.storage.Get("lastLogTerm"); found {
			d := gob.NewDecoder(bytes.NewBuffer(lastLogTermData))
			if err := d.Decode(&cm.lastLogTerm); err != nil {
				dlog.Error("recover runtime data, decode `lastLogTerm` err=%+v", err)
			}
		} else {
			dlog.Error("lastLogTerm not found in storage")
		}
		dlog.Info("runtime data load success")
	} else {
		dlog.Warn("runtime data load failed")
	}
}

// 持久化运行时数据
func (cm *ConsensusModule) persisRuntime() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		dlog.Error("persist runtime data, encode `currentTerm` err=%+v", err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var voteData bytes.Buffer
	if err := gob.NewEncoder(&voteData).Encode(cm.votedFor); err != nil {
		dlog.Error("persist runtime data, encode `votedFor` err=%+v", err)
	}
	cm.storage.Set("votedFor", voteData.Bytes())

	var raftLogData bytes.Buffer
	if err := gob.NewEncoder(&raftLogData).Encode(cm.raftLog); err != nil {
		dlog.Error("persist runtime data, encode `raftLog` err=%+v", err)
	}
	cm.storage.Set("raftLog", raftLogData.Bytes())

	var acData bytes.Buffer
	if err := gob.NewEncoder(&acData).Encode(cm.ac.Value()); err != nil {
		dlog.Error("persist runtime data, encode `ac` err=%+v", err)
	}
	cm.storage.Set("ac", acData.Bytes())

	var persistIndexData bytes.Buffer
	if err := gob.NewEncoder(&persistIndexData).Encode(cm.persistIndex); err != nil {
		dlog.Error("persist runtime data, encode `persistIndex` err=%+v", err)
	}
	cm.storage.Set("persistIndex", persistIndexData.Bytes())

	var lastLogTermData bytes.Buffer
	if err := gob.NewEncoder(&lastLogTermData).Encode(cm.lastLogTerm); err != nil {
		dlog.Error("persist runtime data, encode `lastLogTerm` err=%+v", err)
	}
	cm.storage.Set("lastLogTerm", lastLogTermData.Bytes())

	// 开协程去落盘
	go func() {
		cm.storage.PersistToFile()
	}()
}

// 持久化快照
func (cm *ConsensusModule) persistSnapShot() {
	cm.mu.Lock()
	if cm.state != Leader && cm.state != Follower {
		dlog.Warn("Only Leader and Follower are allowed to persist snapshot, state=%v", cm.state)
		cm.mu.Unlock()
		return
	}
	// 将当前已提交部分持久化
	// 如果持久化进度和提交进度一致则无需重复持久化
	if cm.persistIndex == cm.commitIndex {
		dlog.Info("committed data has been currently persisted, currentTerm=%v,persisIndex=%v",
			cm.currentTerm, cm.persistIndex)
		cm.mu.Unlock()
		return
	}
	dlog.Debug("[before persistence]: currentTerm=%v,persistIndex=%v,commitIndex=%v,raftLog=%v",
		cm.currentTerm, cm.persistIndex, cm.commitIndex, cm.raftLog)
	relCommitIndex := cm.getRelLogIndex(cm.commitIndex)
	cm.persistIndex = cm.commitIndex
	// 丢弃日志
	cm.raftLog = cm.raftLog[relCommitIndex+1:]
	dlog.Debug("[after persistence]: currentTerm=%v,persistIndex=%v,commitIndex=%v,raftLog=%v",
		cm.currentTerm, cm.persistIndex, cm.commitIndex, cm.raftLog)
	// 持久化运行时数据
	cm.persisRuntime()
	if cm.state == Follower && (cm.FileMap == nil || cm.FileMap.Length() == 0) {
		dlog.Warn("state=%+v, FileMap without data, Skip persistence", cm.state)
		cm.mu.Unlock()
		return
	}
	commandMap := make(map[string][]byte)
	var fileMapData bytes.Buffer
	if err := gob.NewEncoder(&fileMapData).Encode(cm.FileMap); err != nil {
		dlog.Error("persist FileMap Error, err=%v", err)
	} else {
		commandMap["FileMap"] = fileMapData.Bytes()
		dlog.Debug("state=%v,currentTerm=%+v，FileMap=%v", cm.state, cm.currentTerm, cm.FileMap)
	}
	cm.mu.Unlock()
	result := cm.storage.SnapShot(commandMap)
	if result {
		dlog.Info("persist success")
	} else {
		dlog.Info("persist failed")
	}
}

// ##################################   选举模块   #######################################################################

// RequestVotedArgs 拉票请求参数
type RequestVotedArgs struct {
	Term              int    // 请求方任期
	CandidateEndpoint string // 当前候选人地址
	// 数据状态
	LastLogIndex int // 最新日志绝对索引
	LastLogTerm  int // 最新日志的任期
}

// RequestVotedReply 拉票响应参数
type RequestVotedReply struct {
	Term         int  // 响应方任期
	VotedGranted bool // 是否同意投票
}

// RequestVote 处理投票请求
func (cm *ConsensusModule) RequestVote(args RequestVotedArgs, reply *RequestVotedReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}
	lastAbsLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	dlog.Debug("RequestVote: args=%+v [currentTerm=%d, votedFor=%d, lastAbsLogIndex=%d, lastLogTerm=%d ]", args, cm.currentTerm, cm.votedFor, lastAbsLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		dlog.Info("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	reply.VotedGranted = false
	if cm.currentTerm == args.Term && // 任期相同
		cm.votedFor == "" || cm.votedFor == args.CandidateEndpoint { // 未将票给其他节点
		if args.LastLogTerm > lastLogTerm || // 最新一条日志的任期更高，数据更新
			args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastAbsLogIndex { // 最新一条日志的任期相同，索引更大则数据更新
			reply.VotedGranted = true
			cm.votedFor = args.CandidateEndpoint
			cm.electionResetEvent = time.Now()
		}
	}

	reply.Term = cm.currentTerm
	// 数据更新需要持久化
	cm.persisRuntime()
	dlog.Debug("... RequestVote reply: %+v", reply)
	return nil
}

// 开启选举
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.endpoint
	dlog.Info("becomes Candidate(currentTerm=%d); raftLog=%v", savedCurrentTerm, cm.raftLog)

	voteReceived := 1
	for _, peerEndpoint := range cm.peerEndpoints {
		go func(peerEndpoint string) {
			cm.mu.Lock()
			lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVotedArgs{
				Term:              savedCurrentTerm,
				CandidateEndpoint: cm.endpoint,
				LastLogIndex:      lastLogIndex,
				LastLogTerm:       lastLogTerm,
			}

			dlog.Debug("sending RequestVote to [%s]: %+v", peerEndpoint, args)
			var reply *RequestVotedReply
			if err := cm.server.Call(peerEndpoint, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				dlog.Debug("received RequestVoteReply %v", reply)

				if cm.state != Candidate {
					dlog.Info("while waiting for reply, state = %v", cm.state)
					return
				}
				if reply.Term > cm.currentTerm {
					dlog.Info("term out of date in RequestVoteReply,term=%d", reply.Term)
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VotedGranted {
						voteReceived += 1
						if voteReceived*2 > len(cm.peerEndpoints)+1 {
							dlog.Info("wins election with %d vote", voteReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerEndpoint)
	}
	// 任期变化会导致原先的定时器协程结束
	// 新启一个计时器避免超时
	go cm.runElectionTimer()
}

// 进入Follower状态
func (cm *ConsensusModule) becomeFollower(term int) {
	dlog.Info("becomes Follower with term=%d; raftLog=%v", term, cm.raftLog)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = ""
	cm.electionResetEvent = time.Now()
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	for _, peerEndpoint := range cm.peerEndpoints {
		cm.nextIndex[peerEndpoint] = cm.getAbsLogIndex(len(cm.raftLog))
		cm.matchIndex[peerEndpoint] = -1
	}
	dlog.Info("becomes Leader; term=%d, absLogLength=%d, commitIndex=%d, persisIndex=%d, FileMap=%v",
		cm.currentTerm, cm.getAbsLogIndex(len(cm.raftLog)), cm.commitIndex, cm.persistIndex, cm.FileMap)

	heartbeatTimeout := common.RAFT_HEARTNBEAT_TIMEOUT * time.Millisecond
	go func(heartbeatTimeout time.Duration) {
		// 发送一次心跳包更新事件
		cm.leaderSendAEs()
		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()

		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}
			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					// 只有Leader需要运行当前同步事件监听协程
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				// 发送心跳包
				cm.leaderSendAEs()
			}
		}
	}(heartbeatTimeout)
}

// ##################################   日志同步模块   #######################################################################

type AppendEntriesArgs struct {
	Term           int    // 发送方任期
	LeaderEndpoint string // Leader的地址

	PreLogIndex   int        // 上一次同步日志的最后一位索引
	PreLogTerm    int        // 上一次同步日志的最后一位任期
	Entries       []LogEntry // 同步的日志内容
	LeaderCommit  int        // Leader的提交进度
	LeaderPersist int        // Leader的持久化进度
	// 只有全量同步使用的数据
	Ac          int64 // 计数器
	LastLogTerm int
	SnapShot    map[string][]byte // 快照数据
}

type AppendEntriesReply struct {
	Term    int  // 响应方任期
	Success bool // 同步结果
	// 用于修正同步数据
	ConflictTerm  int // 冲突的任期
	ConflictIndex int // 冲突的索引
}

// AppendEntries 处理同步日志
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	if args.Term > cm.currentTerm {
		dlog.Info("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}
	reply.Success = false
	var err error
	if args.Ac != -1 {
		err = cm.fullSync(args, reply)
	}
	err = cm.incrementalSync(args, reply)
	reply.Term = cm.currentTerm
	// 持久化运行时数据
	cm.persisRuntime()
	dlog.Info("AppendEntries reply to [%v]", args.LeaderEndpoint)
	return err
}

// 日志全量同步
func (cm *ConsensusModule) fullSync(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		dlog.Info("full sync from [%v]", args.LeaderEndpoint)
		dlog.Debug("args for full sync from [%v]: [args=%+v, commitIndex=%v,raftLog=%+v, persistIndex=%v]",
			args.LeaderEndpoint, args, cm.commitIndex, cm.raftLog, cm.persistIndex)
		cm.electionResetEvent = time.Now()

		// TODO: 读取快照数据

		cm.persistIndex = args.LeaderPersist
		cm.lastLogTerm = args.LastLogTerm
		cm.ac.SetValue(args.Ac)
		cm.commitIndex = args.LeaderCommit
		if len(args.Entries) > 0 {
			cm.raftLog = args.Entries
			dlog.Debug("... full inserting entries raftLog=%+v", cm.raftLog)
		}
		// 持久化的数据实体不是最新的，按顺序重放为被持久化的指令
		if cm.persistIndex < cm.commitIndex {
			// TODO: 执行指令
		}
		dlog.Debug("... setting commitIndex=%d", cm.commitIndex)
		reply.Success = true
		dlog.Debug("after full sync FileMap=%+v", cm.FileMap)
		dlog.Info("full Sync Success")
	}
	return nil
}

// 日志增量复同步
func (cm *ConsensusModule) incrementalSync(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if args.Term == cm.currentTerm && args.LeaderCommit >= cm.commitIndex {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		totalLogLength := cm.getAbsLogIndex(len(cm.raftLog))
		relPreLogIndex := cm.getRelLogIndex(args.PreLogIndex)
		dlog.Debug("Args for incremental sync from [%v]: [args=%+v, totalLogLength=%v,relPreLogIndex=%v,raftLog=%+v, persistIndex=%v,lastLogTerm=%v]",
			args.LeaderEndpoint, args, totalLogLength, relPreLogIndex, cm.raftLog, cm.persistIndex, cm.lastLogTerm)
		if (args.PreLogIndex == -1 && args.LeaderCommit == -1) || // 集群的节点第一次同步
			relPreLogIndex == -1 || // 相对索引为-1，Follower在持久化后第一次同步
			(args.PreLogIndex < totalLogLength && // 对应只提交的情况
				args.LastLogTerm == cm.lastLogTerm &&
				args.PreLogIndex+len(args.Entries)+1 == totalLogLength) ||
			(args.PreLogIndex < totalLogLength && // 增量同步
				relPreLogIndex >= 0 && relPreLogIndex < len(cm.raftLog) &&
				cm.raftLog[relPreLogIndex].Term == args.PreLogTerm) {
			reply.Success = true
			logInsertIndex := relPreLogIndex + 1
			newEntriesIndex := 0
			for {
				if logInsertIndex >= len(cm.raftLog) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.raftLog[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			if newEntriesIndex <= len(args.Entries) {
				dlog.Debug("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.raftLog = append(cm.raftLog[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.lastLogTerm = cm.raftLog[len(cm.raftLog)-1].Term
				dlog.Debug("... log is now: %v", cm.raftLog)
			}
			dlog.Debug("incremental sync commit from [%v]: LeaderCommit=%+v, FollowerCommit=%+v,FileMap=%v",
				args.LeaderEndpoint, args.LeaderEndpoint, cm.commitIndex, cm.FileMap)
			if args.LeaderCommit > cm.commitIndex {
				newTotalLogLength := cm.getAbsLogIndex(len(cm.raftLog))
				cm.commitIndex = common.Min(args.LeaderCommit, newTotalLogLength)
				dlog.Debug("... setting commitIndex=%d", cm.commitIndex)
				// TODO: 执行新同步的日志命令

				dlog.Debug("after commit to [%v]: state=%v, LeaderCommit=%+v, FollowerCommit=%+v,FileMap=%v",
					args.LeaderEndpoint, args.LeaderCommit, cm.commitIndex, cm.FileMap)
			}
		} else { // 数据存在问题
			// 判断是否存在需要全量复制
			if args.PreLogIndex >= totalLogLength || // 当前总日志长度小于上一次提交的索引
				relPreLogIndex < 0 || relPreLogIndex >= len(cm.raftLog) || // 相对索引不存在
				args.PreLogTerm == -1 && args.LastLogTerm != cm.lastLogTerm {
				reply.ConflictIndex = cm.persistIndex + 1 // 从当前持久化处开始发送
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = cm.raftLog[relPreLogIndex].Term
				var i int
				for i = relPreLogIndex - 1; i >= 0; i-- {
					if cm.raftLog[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = cm.getAbsLogIndex(i + 1)
			}
		}

	}
	return nil
}

func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerEndpoint := range cm.peerEndpoints {
		if cm.server.peerClients[peerEndpoint] == nil {
			dlog.Info("losing connection with %+v", peerEndpoint)
			return
		}
		go func(peerEndpoint string) {
			cm.mu.Lock()
			ni := cm.nextIndex[peerEndpoint]
			pervTotalLogIndex := ni - 1
			pervLogIndex := cm.getRelLogIndex(pervTotalLogIndex)
			pervLogTerm := -1
			var ac int64 = -1
			var snapShot map[string][]byte
			var entries []LogEntry
			// 当前日志中找不到，使用全量同步，ac除计数器外可以当作全量同步的标志
			if pervLogIndex < -1 {
				snapShot = cm.storage.getSnapShot()
				ac = cm.ac.Value()
				entries = cm.raftLog
			} else {
				if pervLogIndex != -1 { //存在则带上上一次同步的任期
					pervLogTerm = cm.raftLog[pervLogIndex].Term
				}
				entries = cm.raftLog[pervLogIndex+1:]
			}
			args := AppendEntriesArgs{
				Term:           cm.currentTerm,
				LeaderEndpoint: cm.endpoint,
				PreLogTerm:     pervLogTerm,
				PreLogIndex:    pervLogIndex,
				Entries:        entries,
				LeaderCommit:   cm.commitIndex,
				SnapShot:       snapShot,
				Ac:             ac,
				LeaderPersist:  cm.persistIndex,
				LastLogTerm:    cm.lastLogTerm,
			}
			dlog.Debug("sending AppendEntries to %v: ni=%d,args=%+v,pervLogIndex=%v", peerEndpoint, ni, args, pervLogIndex)
			cm.mu.Unlock()

			var reply AppendEntriesReply
			if err := cm.server.Call(peerEndpoint, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					dlog.Debug("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						if ac != -1 { // 全量同步
							cm.nextIndex[peerEndpoint] = cm.commitIndex + 1 // 只有提交的部分才是真实可信的数据
						} else {
							cm.nextIndex[peerEndpoint] = ni + len(entries)
						}
						cm.matchIndex[peerEndpoint] = cm.nextIndex[peerEndpoint] - 1
						savedAbsCommitIndex := cm.commitIndex
						savedRelCommitIndex := cm.getRelLogIndex(cm.commitIndex)
						for i := savedRelCommitIndex + 1; i < len(cm.raftLog); i++ {
							matchCount := 1
							// 统计当前索引同步成功的个数
							for _, peer := range cm.peerEndpoints {
								if cm.matchIndex[peer] > cm.getAbsLogIndex(i) {
									matchCount++
								}
							}
							// 大多数节点同步成功,提交日志
							if matchCount*2 >= len(cm.peerEndpoints) {
								// 执行
								savedCurrentCommitIndex := cm.commitIndex
								cm.commitIndex = cm.getAbsLogIndex(i)
								// TODO: 执行命令

								dlog.Debug("Leader CommitIndex Update,commitIndex from %v to %v, FileMap=%v",
									savedCurrentCommitIndex, cm.commitIndex, cm.FileMap)
							}
						}
						// 需要提交
						if cm.commitIndex != savedAbsCommitIndex {
							// 通知其他节点提交
							cm.triggerAEChan <- struct{}{}
							dlog.Info("leader sets commitIndex := %d", cm.commitIndex)
						}
					} else { // 失败的情况
						dlog.Debug("AppendEntries reply from %v has conflict, reply=%v", peerEndpoint, reply)
						if reply.ConflictTerm >= 0 { // 存在数据冲突
							// 尝试在当前存在的日志中找到冲突数据位置
							lastTermIndex := -1
							for i := len(cm.raftLog); i >= 0; i-- {
								if cm.raftLog[i].Term == reply.ConflictTerm {
									lastTermIndex = i
									break
								}
							}
							if lastTermIndex >= 0 {
								cm.nextIndex[peerEndpoint] = lastTermIndex + 1
							} else {
								cm.nextIndex[peerEndpoint] = reply.ConflictIndex
							}
						} else { // 数据丢失
							cm.nextIndex[peerEndpoint] = reply.ConflictIndex
						}
					}
				}
			}
		}(peerEndpoint)
	}

}

// ##################################   定时器模块   #######################################################################
func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// 选举定时器
func (cm *ConsensusModule) runElectionTimer() {
	timeDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	dlog.Info("election timer started (%v), term=%d", timeDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C
		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			dlog.Info("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}
		if termStarted != cm.currentTerm {
			dlog.Info("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeDuration {
			// TODO: 开始选举

			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) persistLogTimer() {
	persistTimeout := time.Duration(cm.server.config.PersisTimeout) * time.Millisecond
	t := time.NewTimer(persistTimeout)
	defer t.Stop()
	for {
		doSend := false
		select {
		case <-t.C:
			doSend = true
			// timer是单次作用，所以需要停止再reset
			t.Stop()
			t.Reset(persistTimeout)
		case _, ok := <-cm.persistReadyChan:
			if ok {
				doSend = true
			} else { // channel被关闭
				return
			}
			if !t.Stop() {
				<-t.C
			}
			t.Reset(persistTimeout)
		}
		if doSend {
			cm.persistSnapShot()
		}
	}

}

// ##################################   命令模块   #######################################################################

func (cm *ConsensusModule) dealWithCommands(savedAbsCommitIndex int) {
	savedRefCommitIndex := cm.getRelLogIndex(savedAbsCommitIndex)
	refCommitIndex := cm.getRelLogIndex(cm.commitIndex)
	for i := savedRefCommitIndex + 1; i <= refCommitIndex; i++ {
		//logEntry := cm.raftLog[i]
		// TODO: 解析命令
		//command :=
		// TODO: 执行指令

	}
	// 计数提交指令条数，用于触发持久化
	if cm.ac.AddAndIsTrigger(int64(cm.commitIndex - savedAbsCommitIndex)) {
		cm.persistReadyChan <- struct{}{}
	}
}

func (cm *ConsensusModule) parseCommandFromLog(entry LogEntry) interface{} {
	return entry.Command
}

// ##################################   CM内部通用工具模块   #######################################################################

// 相对索引其实就是raftLog中使用的索引
// 相对索引转绝对索引
func (cm *ConsensusModule) getAbsLogIndex(logIndex int) int {
	return logIndex + cm.persistIndex + 1
}

// 绝对索引转相对索引
func (cm *ConsensusModule) getRelLogIndex(logIndex int) int {
	return logIndex - cm.persistIndex - 1
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.raftLog) > 0 {
		lastIndex := cm.getAbsLogIndex(len(cm.raftLog) - 1)
		return lastIndex, cm.raftLog[lastIndex].Term
	} else {
		return -1, cm.lastLogTerm
	}
}
