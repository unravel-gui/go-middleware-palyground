package part8

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// RPCProxy RPC代理
// 对共识模块进一步封装，可以增加一些额外功能，如: 模拟网络不稳定等
type RPCProxy struct {
	Cm *ConsensusModule
}

// RequestVote 处理拉票请求
// 在封装一层用于模拟网络不稳定的情况
func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.Cm.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.Cm.dlog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.Cm.RequestVote(args, reply)
}

// AppendEntries 处理日志同步请求
// 在封装一层用于模拟网络不稳定的情况
func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.Cm.dlog("drop AppendEntries")
			return fmt.Errorf("drop AppendEntries")
		} else if dice == 8 {
			rpp.Cm.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.Cm.AppendEntries(args, reply)
}
func (rpp *RPCProxy) InstallSnapShot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.Cm.dlog("drop InstallSnapShot")
			return fmt.Errorf("drop InstallSnapShot")
		} else if dice == 8 {
			rpp.Cm.dlog("delay InstallSnapShot")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.Cm.InstallSnapShot(args, reply)
}

type CommandProxy struct {
	s *Server
}

func (cp *CommandProxy) HandleCommand(args CommandArgs, reply *CommandReply) error {
	log.Printf("rpc args:=%v", args)
	defer log.Printf("rpc raft server command reply=%+v", reply)
	return cp.s.handleCommand(args, reply)
}
