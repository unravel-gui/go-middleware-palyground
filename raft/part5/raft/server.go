package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	logger "raft/part5/log"
	"sync"
	"time"
)

var dlog = logger.GetBasicLogger()

type Server struct {
	mu       sync.Mutex
	id       int64
	endpoint string
	servers  map[int64]string

	cm      *ConsensusModule
	persist Persister

	rpcProxy  *RPCProxy
	rpcServer *rpc.Server
	listener  net.Listener

	logger logger.Logger
	ready  <-chan interface{}
	quit   chan interface{}
	wg     sync.WaitGroup
}

func NewServer(endpoint string, servers map[int64]string, persist Persister, ready <-chan interface{}, config *config.RaftConfig) *Server {
	server := new(Server)
	server.endpoint = endpoint
	server.servers = servers
	server.persist = persist
	server.newLogger()
	server.ready = ready
	server.quit = make(chan interface{})
	return server
}
func (s *Server) newLogger() {
	if config.Pattern == "standalone" {
		s.logger = logger.NewRaftLogger(s.id, s.endpoint)
	} else {
		s.logger = logger.GetRaftLogger()
	}
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.id, s.endpoint, s.servers, s, s.persist)
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{
		cm: s.cm,
	}
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", s.config.Endpoint)
	if err != nil {
		s.logger.Fatal(err)
	}
	s.logger.Info("[%v] listening at %s", s.endpoint, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, errAccept := s.listener.Accept()
			if errAccept != nil {
				select {
				case <-s.quit:
					return
				default:
					s.logger.Fatalf("accept error: %v", errAccept)
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
		s.logger.Warn("listener is nil")
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(endpoint string, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[endpoint] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			s.logger.Error("connect error endpoint=%v,err=%v", endpoint, err)
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
	s.cm.Stop()
	close(s.quit)
	s.listener.Close()
	s.logger.Info("raft Server shutdown")
	s.wg.Wait()
}

type RPCProxy struct {
	cm *ConsensusModule
}

func (rpp *RPCProxy) RequestVote(args RequestVotedArgs, reply *RequestVotedReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.logger.Debug("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.logger.Debug("delay RequestVote")
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
			rpp.cm.logger.Info("drop AppendEntries")
			return fmt.Errorf("drop AppendEntries")
		} else if dice == 8 {
			rpp.cm.logger.Info("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}
