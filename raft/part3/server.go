package part3

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Server Raft服务器
type Server struct {
	mu sync.Mutex // 保证高并发的数据安全

	serverId int   // 当前节点id
	peerIds  []int // 其他节点Id列表

	cm       *ConsensusModule // 共识模块
	storage  Storage          // 存储
	rpcProxy *RPCProxy        // rpc代理，本质是再共识模块的基础上封装一层功能

	rpcServer *rpc.Server  // rpc服务器对象，用于提供rpc服务
	listener  net.Listener // 当前raft服务器的监听器

	commitChan  chan<- CommitEntry  // 传输已提交日志的channel
	peerClients map[int]*rpc.Client // 其他节点的客户端，用于调度其他节点的rpc服务

	ready <-chan interface{} // 用于通知服务是否准备好启动
	quit  chan interface{}   // 用于通知服务是否准备好关闭
	wg    sync.WaitGroup     // 用于等待其他协程结束
}

func NewServer(serverId int, peerIds []int, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.storage = storage
	s.ready = ready
	s.commitChan = commitChan
	s.quit = make(chan interface{})
	return s
}

// Serve 启动服务器
func (s *Server) Serve() {
	s.mu.Lock()
	// 创建共识模块
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.storage, s.ready, s.commitChan)
	// 创建rpc服务器(开启RPC服务)
	s.rpcServer = rpc.NewServer()
	// 创建代理对象
	s.rpcProxy = &RPCProxy{cm: s.cm}
	// 将共识模块注册到服务中(共识模块中的拉票以及同步日志服务)
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	// 创建监听器
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()
	// 用于下面的监听逻辑
	s.wg.Add(1)

	go func() {
		// 任务结束时标记完成，对应上层的Add(1)
		defer s.wg.Done()
		for {
			// 获得连接对象
			conn, err := s.listener.Accept()
			// 出其他错误外，监听器关闭时，也会抛出错误
			if err != nil {
				select {
				case <-s.quit: // 监听退出 channel，可以得知服务是否关闭
					return
				default: // 服务未被告知退出，则打印异常信息
					log.Fatal("accept error:", err)
				}
			}
			// 未新开的附属协程+1
			s.wg.Add(1)
			go func() {
				// 处理连接
				s.rpcServer.ServeConn(conn)
				// 标记任务完成
				s.wg.Done()
			}()
		}
	}()
}

// DisconnectAll 断开所有连接(断开所有节点的客户端)
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

// Shutdown 关闭服务器
func (s *Server) Shutdown() {
	// 关闭共识模块
	s.cm.Stop()
	// 这里必须先关闭 quit channel，再关闭监听器
	// 关闭监听器后Accept函数会报错，然后才会进入到监听quit channel的退出逻辑
	// 相反则会打印报错,在第二次监听后在退出(不够优雅)
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

// GetListenAddr 节点的地址信息
func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

// ConnectToPeer 连接指定节点(获得指定节点的客户端)
func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

// DisconnectPeer 断开指定节点的连接(关闭节点客户端)
func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

// Call 远程调用指定节点的服务
func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}

}

// RPCProxy RPC代理
// 对共识模块进一步封装，可以增加一些额外功能，如: 模拟网络不稳定等
type RPCProxy struct {
	cm *ConsensusModule
}

// RequestVote 处理拉票请求
// 在封装一层用于模拟网络不稳定的情况
func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}

// AppendEntries 处理日志同步请求
// 在封装一层用于模拟网络不稳定的情况
func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.dlog("drop AppendEntries")
			return fmt.Errorf("drop AppendEntries")
		} else if dice == 8 {
			rpp.cm.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}
