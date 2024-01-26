package part8

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"raft/part8/common"
	"sync"
	"time"
)

var idGenerator = common.GlobalIDGenerator

type OperationRecord struct {
	CommandId int
	Reply     CommandReply
}

func NewOpRecord(cId int, reply CommandReply) OperationRecord {
	return OperationRecord{
		CommandId: cId,
		Reply:     reply,
	}
}

type OperationCache map[int]OperationRecord

// Server Raft服务器
type Server struct {
	mu sync.Mutex // 保证高并发的数据安全

	serverId int // 当前节点id
	endpoint string
	peerIds  map[int]string // 其他节点Id列表

	cm           *ConsensusModule // 共识模块
	storage      *Storage         // 存储
	rpcProxy     *RPCProxy        // rpc代理，本质是再共识模块的基础上封装一层功能
	commandProxy *CommandProxy

	rpcServer *rpc.Server  // rpc服务器对象，用于提供rpc服务
	listener  net.Listener // 当前raft服务器的监听器

	applyChan   chan ApplyMsg       // 传输已提交日志的channel
	peerClients map[int]*rpc.Client // 其他节点的客户端，用于调度其他节点的rpc服务

	kvMap         map[string]string
	nofiyChans    map[int]chan CommandReply
	lastApplied   int
	lastOperation OperationCache
	maxRaftState  int

	ready    <-chan interface{} // 用于通知服务是否准备好启动
	shutdown *common.AtomicBool // 标记服务器是否关闭
	wg       sync.WaitGroup     // 用于等待其他协程结束
}

func NewServer(serverId int, endpoint string, peerIds map[int]string, storage *Storage) *Server {
	s := new(Server)
	s.serverId = serverId
	s.endpoint = endpoint
	s.peerIds = peerIds
	s.lastApplied = 0
	s.peerClients = make(map[int]*rpc.Client)
	s.kvMap = make(map[string]string)
	s.nofiyChans = make(map[int]chan CommandReply, 0)
	s.lastOperation = make(OperationCache)
	s.storage = storage
	s.maxRaftState = 500
	s.applyChan = make(chan ApplyMsg)
	s.shutdown = common.NewAtomicBool(false)
	return s
}

// Serve 启动服务器
func (s *Server) Serve() {
	// 创建共识模块
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.storage, s.applyChan)
	// 创建rpc服务器(开启RPC服务)
	s.rpcServer = rpc.NewServer()
	// 创建代理对象
	s.rpcProxy = &RPCProxy{Cm: s.cm}
	// 将共识模块注册到服务中(共识模块中的拉票以及同步日志服务)
	s.RegisterName("ConsensusModule", s.rpcProxy)
	s.commandProxy = &CommandProxy{s: s}
	s.RegisterName("Command", s.commandProxy)
	var err error
	// 创建监听器
	s.listener, err = net.Listen("tcp", s.endpoint)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	// 用于下面的监听逻辑
	s.wg.Add(1)

	go func() {
		// 任务结束时标记完成，对应上层的Add(1)
		defer s.wg.Done()
		for {
			// 获得连接对象
			conn, err := s.listener.Accept()
			// 出其他错误外，监听器关闭时，也会抛出错误
			if s.isStop() {
				s.dlog("[shutdown] rpc is down...")
				return
			}
			if err != nil {
				log.Fatal("accept error:", err)
			}
			s.dlog("[RPC] get conn:%+v \n", conn)
			s.wg.Add(1)
			go func() {
				// 处理连接
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()

	go s.applier()
}

func (s *Server) isStop() bool {
	return s.shutdown.Get()
}

// Shutdown 关闭服务器
func (s *Server) Shutdown() {
	s.mu.Lock()
	// 关闭共识模块
	s.cm.Stop()

	s.shutdown.Set(true)
	s.listener.Close()
	// 清除未使用的chan
	for _, replies := range s.nofiyChans {
		close(replies)
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Server) RegisterName(service string, obj any) {
	s.rpcServer.RegisterName(service, obj)
}

// DisconnectAll 断开所有连接(断开所有节点的客户端)
func (s *Server) DisconnectAll() {
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
	s.peerClients = make(map[int]*rpc.Client, 0)
}

// ConnectToPeer 连接指定节点(获得指定节点的客户端)
func (s *Server) ConnectToPeer(peerId int, addr string) error {
	s.mu.Lock()
	if s.peerClients[peerId] == nil {
		s.mu.Unlock()
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.peerClients[peerId] = client
		s.mu.Unlock()
	}
	return nil
}

func (s *Server) ReConnectToPeer(peerId int) error {
	s.mu.Lock()
	endpoint := s.peerIds[peerId]
	s.mu.Unlock()
	if endpoint == "" {
		return nil
	}
	client, err := rpc.Dial("tcp", endpoint)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.peerClients[peerId] = client
	s.mu.Unlock()
	return nil
}

// DisconnectPeer 断开指定节点的连接(关闭节点客户端)
func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		delete(s.peerClients, peerId)
		return err
	}

	return nil
}

func (s *Server) connect(peerId int) (*rpc.Client, error) {
	if peer, exists := s.peerClients[peerId]; exists {
		return peer, nil
	}
	// 服务停止关闭自动连接
	if s.isStop() {
		return nil, nil
	}
	// 是否设置关闭自动连接
	if len(os.Getenv("RaftServerTest")) > 0 {
		return nil, nil
	}

	s.dlog("try to connect Node[%v]", peerId)
	var client *rpc.Client
	var err error
	// 尝试连接
	for i := 0; i < 3; i++ {
		client, err = rpc.Dial("tcp", s.peerIds[peerId])
		if err == nil {
			s.peerClients[peerId] = client
			return client, nil
		}
	}
	s.dlog("lost connect with Node[%v]", peerId)
	return nil, err
}

// Call 远程调用指定节点的服务
func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	// 如果程序已经停止则停止远程调用
	if s.isStop() {
		return nil
	}
	s.mu.Lock()
	peer, err := s.connect(id)
	s.mu.Unlock()
	if err != nil {
		s.dlog("connect to Node[%v] err:%+v", id, err)
		return err
	}
	//s.cm.dlog("Node[%v] call Node[%v].%s", s.cm.id, id, serviceMethod)
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

func (s *Server) Report() (int, int, bool) {
	return s.cm.Report()
}

func (s *Server) GetState() CMState {
	return s.cm.GetState()
}

func (s *Server) GetLeaderId() int {
	return s.cm.GetLeaderId()
}

func (s *Server) Get(key string, reply *CommandReply) error {
	args := CommandArgs{
		Op:        GET,
		Key:       key,
		CommandId: idGenerator.GenerateID(),
	}
	s.handleCommand(args, reply)
	return nil
}

func (s *Server) Put(key, value string, reply *CommandReply) error {
	args := CommandArgs{
		Op:        PUT,
		Key:       key,
		Value:     value,
		CommandId: idGenerator.GenerateID(),
	}
	s.handleCommand(args, reply)
	return nil
}

func (s *Server) Delete(key string, reply *CommandReply) error {
	args := CommandArgs{
		Op:        DELETE,
		Key:       key,
		CommandId: idGenerator.GenerateID(),
	}
	s.handleCommand(args, reply)
	return nil
}

func (s *Server) Clear(reply *CommandReply) {
	args := CommandArgs{
		Op:        CLEAR,
		CommandId: idGenerator.GenerateID(),
	}
	s.handleCommand(args, reply)
}
func (s *Server) HandleCommand(args CommandArgs, reply *CommandReply) error {
	s.dlog("local execute from http server,args=%+v", args)
	defer s.dlog("local execute reply is %+v", reply)
	return s.handleCommand(args, reply)
}

func (s *Server) handleCommand(args CommandArgs, reply *CommandReply) error {
	defer func() {
		s.dlog("Node[%d] processes CommandArgs %+v with CommandReply %+v\n", s.serverId, args, reply)
	}()

	// 写入集群日志
	logIndex, ok := s.Submit(args)
	if !ok {
		// 非 leader 节点判断
		reply.CmdStatus = WRONG_LEADER
		reply.LeaderId = s.cm.GetLeaderId()
		return nil
	}
	// 获得传递响应的chan
	s.mu.Lock()
	// 需要阻塞等待
	chanLock := make(chan CommandReply)
	s.nofiyChans[logIndex] = chanLock
	s.mu.Unlock()
	// 等待raft集群操作
	timeout := time.Duration(1000) * time.Millisecond
	select {
	case resp := <-chanLock: // 成功获得响应
		*reply = resp
	case <-time.After(timeout): // 集群响应超时
		reply.CmdStatus = TIMEOUT
	}

	s.mu.Lock()
	// 使用后删除chan，每个index对应一个chan
	delete(s.nofiyChans, logIndex)
	s.mu.Unlock()
	return nil
}

func (s *Server) applier() {
	for msg := range s.applyChan {
		s.mu.Lock()
		s.dlog("Node[%d] tries to apply message %+v\n", s.serverId, msg)

		switch msg.Type {
		case SNAPSHOT: // 读取快照
			s.dlog("full sync with snapshot")
			// 组成快照
			snap := NewSnapShot(msg.Index, msg.Term, msg.Command)
			s.cm.persistSnapShot(snap)
			s.readSnapShot(snap)
			s.lastApplied = msg.Index
		case ENTRY: // 应用日志
			if msg.Command == nil {
				continue
			}
			msgIndex := msg.Index
			if msgIndex <= s.lastApplied {
				s.dlog("Node[%d] discards outdated message %+v because a newer snapshot which lastApplied is %d has been restored\n",
					s.serverId, msg, s.lastApplied)
				continue
			}
			s.lastApplied = msgIndex
			// 解析成请求
			var args CommandArgs
			err := s.recoverCmdArgs(msg.Command, &args)
			if err != nil {
				s.dlog("recover CommandArgs err:%v", err)
				return
			}
			var reply CommandReply
			// 应用到状态机
			s.applyLogToStateMachine(args, &reply)
			// 获得当前raft集群状态
			_, term, isLeader := s.cm.Report()
			if isLeader && msg.Term == term { // 是当前任期的Leader操作
				reply.LeaderId = s.serverId
				// 返回响应
				if notify, exists := s.nofiyChans[msg.Index]; exists {
					notify <- reply
				}
			}
			// 判断是否需要持久化
			if s.needSnapShot() {
				s.saveSnapShot(msg.Index)
			}
		default:
			s.dlog("Unexpected ApplyMsg type: %d, index: %d, term: %d\n", msg.Type, msg.Index, msg.Term)
			s.mu.Unlock()
			os.Exit(-1)
		}
		s.mu.Unlock()
	}
}

func (s *Server) Submit(args CommandArgs) (int, bool) {
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(args); err != nil {
		s.dlog("submit err: %+v", err)
		return -1, false
	}
	return s.cm.Submit(data.Bytes())
}

func (s *Server) recoverCmdArgs(data []byte, msg *CommandArgs) error {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&msg); err != nil {
		return err
	}
	return nil
}

func (kv *Server) applyLogToStateMachine(args CommandArgs, reply *CommandReply) error {
	reply.CmdStatus = OK
	switch args.Op {
	case GET: // 读map
		value, exists := kv.kvMap[args.Key]
		if !exists {
			reply.CmdStatus = NO_KEY
		} else {
			reply.Value = value
		}
	case PUT:
		kv.kvMap[args.Key] = args.Value
	case DELETE:
		_, exists := kv.kvMap[args.Key]
		if !exists {
			reply.CmdStatus = NO_KEY
		} else {
			delete(kv.kvMap, args.Key)
		}
	case CLEAR:
		kv.kvMap = make(map[string]string, 0)
	default:
		os.Exit(-1)
	}
	return nil
}

func (s *Server) isDuplicateargs(client int, command int) bool {
	if entry, exists := s.lastOperation[client]; exists {
		return entry.CommandId == command
	}
	return false
}

func (s *Server) needSnapShot() bool {
	if s.maxRaftState == -1 {
		return false
	}
	return s.storage.GetRaftStateSize() >= s.maxRaftState
}

func (s *Server) readSnapShot(snap *SnapShot) {
	if snap == nil {
		return
	}
	// 持久化数据
	var tmpData *TmpSnapShot
	if err := gob.NewDecoder(bytes.NewReader(snap.Data)).Decode(&tmpData); err != nil {
		fmt.Print(err)
		return
	}
	s.kvMap = tmpData.Data
	s.lastOperation = tmpData.LastOperation
}

func (s *Server) saveSnapShot(index int) {
	tmpSnapShot := TmpSnapShot{
		Data:          s.kvMap,
		LastOperation: s.lastOperation,
	}
	var snapShotBuf bytes.Buffer
	if err := gob.NewEncoder(&snapShotBuf).Encode(tmpSnapShot); err != nil {
		fmt.Print(err)
		return
	}
	s.cm.persistStateAndSnapShot(index, snapShotBuf.Bytes())
}

type TmpSnapShot struct {
	Data          map[string]string
	LastOperation OperationCache
}

func (s *Server) dlog(format string, args ...interface{}) {
	format = fmt.Sprintf("[%d] ", s.serverId) + format
	log.Printf(format, args...)
}

func (s *Server) GetForTest(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, exists := s.kvMap[key]
	return v, exists
}
