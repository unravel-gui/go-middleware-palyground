package part6

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	logger "raft/part6/log"
	"sync"
	"time"
)

// Server Raft服务器
type Server struct {
	mu sync.Mutex // 保证高并发的数据安全

	serverId int64 // 当前节点id
	endpoint string
	peerIds  map[int64]string // 其他节点Id列表

	cm       *ConsensusModule // 共识模块
	storage  *Persister       // 存储
	rpcProxy *RPCProxy        // rpc代理，本质是再共识模块的基础上封装一层功能

	rpcServer *rpc.Server  // rpc服务器对象，用于提供rpc服务
	listener  net.Listener // 当前raft服务器的监听器

	appliedChan chan<- ApplyMsg // 传输已提交日志的channel

	dlog  logger.Logger
	ready <-chan interface{} // 用于通知服务是否准备好启动
	quit  chan interface{}   // 用于通知服务是否准备好关闭
	wg    sync.WaitGroup     // 用于等待其他协程结束
}

func NewServer(serverId int64, endpoint string, peerIds map[int64]string, storage *Persister,
	ready <-chan interface{}, applied chan<- ApplyMsg) *Server {
	s := new(Server)
	s.serverId = serverId
	s.endpoint = endpoint
	s.peerIds = peerIds
	s.storage = storage
	s.newLogger()
	s.ready = ready
	s.appliedChan = applied
	s.quit = make(chan interface{})
	return s
}

// Serve 启动服务器
func (s *Server) Serve() {
	s.mu.Lock()
	// 创建共识模块
	s.cm = NewConsensusModule(s.serverId, s.endpoint, s, s.storage, s.ready, s.appliedChan, s.dlog)
	// 创建rpc服务器(开启RPC服务)
	s.rpcServer = rpc.NewServer()
	// 创建代理对象
	s.rpcProxy = &RPCProxy{cm: s.cm}
	// 将共识模块注册到服务中(共识模块中的拉票以及同步日志服务)
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	// 创建监听器
	s.listener, err = net.Listen("tcp", s.endpoint)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()
	// 用于下面的监听逻辑
	s.wg.Add(1)
	s.dlog.Warn("run wg accept")
	go func() {
		fmt.Printf("accept000000000000\n")
		defer fmt.Printf("accept1111111111111\n")
		// 任务结束时标记完成，对应上层的Add(1)
		defer s.wg.Done()
		for {
			// 获得连接对象
			conn, errApt := s.listener.Accept()
			// 出其他错误外，监听器关闭时，也会抛出错误
			if errApt != nil {
				select {
				case <-s.quit: // 监听退出 channel，可以得知服务是否关闭
					s.dlog.Info("rpc server closing...")
					s.dlog.Warn("run done accept")
					return
				default: // 服务未被告知退出，则打印异常信息
					s.dlog.Fatalf("accept error:", errApt)
				}
			}
			// 未新开的附属协程+1
			s.wg.Add(1)
			s.dlog.Warn("run wg serve")
			go func() {
				fmt.Printf("serve000000000000")
				defer fmt.Printf("serve1111111111111")
				defer s.wg.Done()
				// 处理连接
				s.rpcServer.ServeConn(conn)
				// 标记任务完成
				s.dlog.Warn("run done serve")
			}()
		}
	}()
}

func (s *Server) newLogger() {
	if config.Pattern == "standalone" {
		s.dlog = logger.NewRaftLogger(s.serverId, s.endpoint)
	} else {
		s.dlog = logger.GetRaftLogger()
	}
}

// Shutdown 关闭服务器
func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 关闭共识模块
	s.cm.Stop()
	close(s.quit)
	err := s.listener.Close()
	if err != nil {
		s.dlog.Error("close listener err:%v", err)
		return
	}
	s.dlog.Info("listener is closed")
	s.wg.Wait()
	return
}

// GetListenAddr 节点的地址信息
func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

// DisconnectAll 断开所有连接(断开所有节点的客户端)
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, _ := range s.peerIds {
		s.cm.RemoveRaftPeer(id)
	}
	s.peerIds = make(map[int64]string, 0)
}

// ConnectToPeer 连接指定节点(获得指定节点的客户端)
func (s *Server) ConnectToPeer(peerId int64, endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cm.AddRaftPeer(peerId, endpoint) {
		s.peerIds[peerId] = endpoint
		return nil
	} else {
		return fmt.Errorf("node [%v] is existed", peerId)
	}
}

// DisconnectPeer 断开指定节点的连接(关闭节点客户端)
func (s *Server) DisconnectPeer(peerId int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.peerIds[peerId]; !exists {
		return fmt.Errorf("node [%v] is not existed", peerId)
	}
	delete(s.peerIds, peerId)
	s.cm.RemoveRaftPeer(peerId)
	return nil
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
			rpp.cm.dlog.Info("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlog.Info("delay RequestVote")
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
			rpp.cm.dlog.Info("drop AppendEntries")
			return fmt.Errorf("drop AppendEntries")
		} else if dice == 8 {
			rpp.cm.dlog.Info("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}
