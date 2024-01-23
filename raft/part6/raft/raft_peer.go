package part6

import (
	"fmt"
	"net"
	"net/rpc"
	"raft/part5/common"
	conf "raft/part5/config"
	"strconv"
)

var config = conf.DefaultConfig

type RaftPeer struct {
	id        int64
	rpcClient *rpc.Client
	endpoint  net.Addr
}

func NewTcpAddr(ip string, port int64) net.Addr {
	return &net.TCPAddr{
		IP:   net.ParseIP(ip),
		Port: int(port),
	}
}

func NewRaftPeer(id int64, endpoint string) (*RaftPeer, error) {
	ip, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		return nil, err
	}
	port, err := strconv.ParseInt(portStr, 10, 64)
	// 对client做初始化配置
	// ...
	addr := NewTcpAddr(ip, port)
	return &RaftPeer{
		id:        id,
		rpcClient: nil,
		endpoint:  addr,
	}, err
}

func (rp *RaftPeer) connect() bool {
	// 如果未被关闭
	if rp.rpcClient != nil {
		return true
	}
	for i := 0; i < int(config.Retry); i++ {
		client, err := rpc.Dial(rp.endpoint.Network(), rp.endpoint.String())
		if err == nil {
			rp.rpcClient = client
			dlog.Info("connect node[%v] success", rp.endpoint)
			// 连接成功
			return true
		}
		dlog.Error("connect node[%v] err:%v", rp.endpoint, err)
		common.SleepMs(10)
	}
	return false
}

type RequestVoteArgs struct {
	Term         int64 // 候选人的任期
	CandidateId  int64 // 请求选票的候选人的 ID
	LastLogIndex int64 // 候选人的最后日志条目的索引值
	LastLogTerm  int64 // 候选人的最后日志条目的任期号
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf(
		"RequestVoteArgs{Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d}",
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm,
	)
}

type RequestVoteReply struct {
	Term        int64
	LeaderId    int64
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf(
		"RequestVoteReply{Term: %d, LeaderId: %d, VotedGrand: %t}",
		reply.Term, reply.LeaderId, reply.VoteGranted,
	)
}

func (rp *RaftPeer) requestVote(args interface{}, reply interface{}) error {
	dlog.Info("requestVote begin connect raft peer:%+v", rp)
	if !rp.connect() {
		return fmt.Errorf("losing connection, raftPeer=%+v", rp)
	}
	if err := rp.rpcClient.Call("ConsensusModule.RequestVote", args, reply); err != nil {
		return err
	}
	return nil
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf(
		"AppendEntriesArgs{Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %v, LeaderCommit: %d}",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit,
	)
}

type AppendEntriesReply struct {
	Success   bool
	Term      int64
	LeaderId  int64
	NextIndex int64
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf(
		"AppendEntriesReply{Success: %t, Term: %d, LeaderId: %d, NextIndex: %d}",
		reply.Success, reply.Term, reply.LeaderId, reply.NextIndex,
	)
}
func (rp *RaftPeer) appendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	//dlog.Info("appendEntries begin connect raft peer:%+v", rp)
	if !rp.connect() {
		return fmt.Errorf("losing connection, raftPeer=%+v", rp)
	}
	if err := rp.rpcClient.Call("ConsensusModule.AppendEntries", args, reply); err != nil {
		return err
	}
	return nil
}

type InstallSnapshotArgs struct {
	Term     int64
	LeaderId int64
	Snapshot *SnapShot
}

func (args InstallSnapshotArgs) String() string {
	return fmt.Sprintf(
		"InstallSnapshotArgs{Term: %d, LeaderId: %d, Snapshot: %v}",
		args.Term, args.LeaderId, args.Snapshot,
	)
}

type InstallSnapshotReply struct {
	Term     int64
	LeaderId int64
}

func (reply InstallSnapshotReply) String() string {
	return fmt.Sprintf(
		"InstallSnapshotReply{Term: %d, LeaderId: %d}",
		reply.Term, reply.LeaderId,
	)
}
func (rp *RaftPeer) installSnapShot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	dlog.Info("installSnapShot begin connect raft peer:%+v", rp)
	if !rp.connect() {
		return fmt.Errorf("losing connection, raftPeer=%+v", rp)
	}
	if err := rp.rpcClient.Call("RaftNode.HandleInstallSnapShot", args, reply); err != nil {
		return err
	}
	return nil
}
