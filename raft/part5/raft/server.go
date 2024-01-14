package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	config "raft/part5"
	"sync"
	"time"
)

type Server struct {
	mu            sync.Mutex
	endpoint      string
	peerEndpoints []string

	cm      *ConsensusModule
	storage Storage

	rpcProxy    *RPCProxy
	rpcServer   *rpc.Server
	listener    net.Listener
	peerClients map[string]*rpc.Client

	config config.RaftConfig
	ready  <-chan interface{}
	quit   chan interface{}
	wg     sync.WaitGroup
}

func NewServer(endpoint string, peerEndpoints []string, storage Storage, ready <-chan interface{}, config config.RaftConfig) *Server {
	server := new(Server)
	server.endpoint = endpoint
	server.peerEndpoints = peerEndpoints
	server.peerClients = make(map[string]*rpc.Client)
	server.storage = storage
	server.ready = ready
	server.quit = make(chan interface{})
	server.config = config
	return server
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.endpoint, s.peerEndpoints, s, s.storage, s.ready)
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{
		cm: s.cm,
	}
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", s.config.Endpoint)
	if err != nil {
		dlog.Fatal(err)
	}
	dlog.Info("[%v] listening at %s", s.endpoint, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					dlog.Fatalf("accept error: %v", err)
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

func (s *Server) Call(endpoint string, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	client := s.peerClients[endpoint]
	s.mu.Unlock()

	if client == nil {
		return fmt.Errorf("call client [%s] after it's closed", endpoint)
	} else {
		return client.Call(serviceMethod, args, reply)
	}
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		dlog.Warn("listener is nil")
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(endpoint string, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[endpoint] == nil {
		client, err := rpc.Dial(addr.Network(), addr.Network())
		if err != nil {
			return err
		}
		s.peerClients[endpoint] = client
	}
	return nil
}
func (s *Server) DisconnectPeer(endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[endpoint] != nil {
		err := s.peerClients[endpoint].Close()
		s.peerClients[endpoint] = nil
		return err
	}
	return nil
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for endpoint, client := range s.peerClients {
		if client != nil {
			client.Close()
			s.peerClients[endpoint] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listener.Close()
	s.wg.Wait()
}

type RPCProxy struct {
	cm *ConsensusModule
}

func (rpp *RPCProxy) RequestVote(args RequestVotedArgs, reply *RequestVotedReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			dlog.Debug("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			dlog.Debug("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			dlog.Info("drop AppendEntries")
			return fmt.Errorf("drop AppendEntries")
		} else if dice == 8 {
			dlog.Info("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}
