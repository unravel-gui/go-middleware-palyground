package part1

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

// Server raft服务器结构定义
type Server struct {
	mu sync.Mutex // 用于保证在多线程的状态下raft中数据安全

	serverId int   // 当前服务器的id
	peerIds  []int // 当前服务器集群的id列表

	cm       *ConsensusModule
	rpcProxy *RPCProxy // rpc调度代理

	rpcServer *rpc.Server  // 自身rpc服务器对象，用于接收其他rpc调度
	listener  net.Listener // 监听器

	peerClients map[int]*rpc.Client // 每个服务器的客户端，用于调度其他服务器节点

	ready <-chan interface{} //	只读channel
	quit  chan interface{}   // 是否退出
	wg    sync.WaitGroup     // 等待组，为了数据安全和等待执行完毕
}

// NewServer 创建raft服务器对象
func NewServer(serverId int, peerIds []int, ready <-chan interface{}) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan interface{})
	return s
}

// Serve 启动raft服务器
func (s *Server) Serve() {
	s.mu.Lock()
	// 创建共识模块
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.ready)
	// 启动rpc服务
	s.rpcServer = rpc.NewServer()
	// 获得rpc代理对象
	s.rpcProxy = &RPCProxy{
		cm: s.cm,
	}
	// 注册自己的服务
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	// 监听端口
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()
	s.wg.Add(1)

	go func() {
		defer s.wg.Done() // 当前协程结束时会结束之前在上面+1的任务
		for {
			// 建立连接
			conn, err := s.listener.Accept()
			if err != nil { // 出错则判断是否需要停止
				select {
				case <-s.quit: // 服务停止，退出函数
					return
				default: // 出错打印日志
					log.Fatal("accept error: ", err)
				}
			}
			s.wg.Add(1)
			go func() { // 开启协程处理连接
				s.rpcServer.ServeConn(conn)
				s.wg.Done() // 处理完连接任务完成对应上面的wg +1
			}()
		}
	}()

}

// DisconnectAll 断开所有的peer客户端连接
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			// 关闭连接并置空
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

// Shutdown 停止服务器
func (s *Server) Shutdown() {
	s.cm.Stop()
	// 关闭chan的时候会自动发送零值
	close(s.quit)
	// 关闭监听
	s.listener.Close()
	s.wg.Wait()
}

// GetListenAddr 获得服务器监听地址
func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

// ConnectToPeer 与peer节点建立连接
func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil { // 连接不存在
		// 创建连接
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		// 加入server端维护的客户端列表中
		s.peerClients[peerId] = client
	}
	return nil
}

// DisconnectPeer 断开指定peer连接
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

// Call 封装调用函数，方便使用
func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	// 获得peer客户端对象
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil { // 客户端不存在就打日志
		return fmt.Errorf("call client %d after it's closed", id)
	}
	// 使用客户端调用对应的方法
	return peer.Call(serviceMethod, args, reply)
}

// RPCProxy rpc代理
type RPCProxy struct {
	cm *ConsensusModule // 共识模块
}

// RequestVote 处理拉票请求
func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	// 主打一个随机
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 { // 是否开启这个不可靠的rpc设置,模拟不可靠网络
		dice := rand.Intn(10)
		if dice == 9 { // 放弃这次拉票
			rpp.cm.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 { // 延迟本次拉票
			rpp.cm.dlog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		// 随机延迟拉票时间
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	// 调用拉票请求
	return rpp.cm.RequestVote(args, reply)
}

// AppendEntries 处理同步日志请求
func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.dlog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}
