package httpServer

import (
	"raft/part8"
)

type StartNode struct {
	id       int
	endpoint string
	peerIds  map[int]string
	hs       *HTTPServer
	s        *part8.Server
}

func NewStartNode(id int, port int, peerIds map[int]string, filePath string) *StartNode {
	storage, _, err := part8.NewStorage(filePath)
	if err != nil {
		return nil
	}
	sd := new(StartNode)
	sd.id = id
	sd.endpoint = GetEndpoint(port + 10)
	sd.peerIds = peerIds
	sd.s = part8.NewServer(id, GetEndpoint(port), peerIds, storage)
	sd.hs = NewHTTPServer(id, sd.endpoint, sd.s)
	return sd
}

func (sd *StartNode) Report() (int, int, bool) {
	return sd.s.Report()
}

func (sd *StartNode) getState() part8.CMState {
	return sd.s.GetState()
}

func (sd *StartNode) Start() {
	sd.s.Serve()
	go sd.hs.Start()
	// 等待程序连接
}
func (sd *StartNode) GetForTest(key string) (reply part8.CommandReply) {
	sd.s.Get(key, &reply)
	return
}
func (sd *StartNode) PutForTest(key, value string) (reply part8.CommandReply) {
	sd.s.Put(key, value, &reply)
	return
}
func (sd *StartNode) Shutdown() {
	sd.s.DisconnectAll()
	sd.s.Shutdown()
	sd.hs.Shutdown()
}
