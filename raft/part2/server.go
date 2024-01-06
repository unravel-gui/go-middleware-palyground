package part2

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

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	cm       *ConsensusModule
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	commitChan  chan<- CommitEntry // 只写channel
	peerClients map[int]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}

	wg sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, ready <-chan interface{}, commitChan chan<- CommitEntry) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.commitChan = commitChan
	s.quit = make(chan interface{})
	return s
}

// Serve 开启服务
func (s *Server) Serve() {
	s.mu.Lock()
	// 创建共识模块对象
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.ready, s.commitChan)
	// 创建rpc服务器(开启rpc服务)
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{
		cm: s.cm,
	}
	// 将自己的功能注册到rpc中(实际上即共识模块实现的投票和同步日志的服务)
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	// 创建监听器对象
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] istening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()
	// 用于给下面建立连接的协程
	s.wg.Add(1)

	go func() {
		// 结束时表示当前附属协程结束
		defer s.wg.Done()

		for {
			// 获得连接
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

// DisconnectAll 断开与其他节点的客户端连接
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
	s.cm.Stop()
	// 这里必须先给退出channel中发送信息，在关闭监听器
	// 因为关闭监听器报错后才会进入到监听s.quit channel的逻辑
	close(s.quit)
	s.listener.Close()
	// 阻塞等待所有附属协程跑完
	s.wg.Wait()
}

// GetListenAddr 获得监听地址
func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

// ConnectToPeer 连接指定的节点(获得其客户端对象)
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

// DisconnectPeer 断开指定节点的连接
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

// Call 远程调用指定的节点服务
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
type RPCProxy struct {
	cm *ConsensusModule
}

// RequestVote 处理拉票请求
// 实际上是封装了一层延迟处理，方便模拟网络不稳定的情况
func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RFAT_UNRELIABLE_RPC")) > 0 {
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

// AppendEntries 处理日志同步的请求
// 实际封装一层用于模拟网络不稳定的情况
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
