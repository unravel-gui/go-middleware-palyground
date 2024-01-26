package httpServer

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net"
	"net/http"
	"raft/part8"
	"raft/part8/common"
	"sync"
	"syscall"
	"time"
)

var idGenerator = common.GlobalIDGenerator

type HTTPServer struct {
	mu           sync.Mutex
	id           int
	endpoint     string
	engine       *gin.Engine
	server       *http.Server
	s            *part8.Server
	listener     net.Listener
	shutdownChan chan interface{}
	leaderId     int // raft集群中leader的Id，默认为自己，后续使用时会更新
}

func NewHTTPServer(id int, endpoint string, s *part8.Server) *HTTPServer {
	// 创建 Gin 引擎
	hs := new(HTTPServer)
	hs.id = id
	hs.endpoint = endpoint
	hs.s = s
	hs.engine = gin.Default()
	hs.server = &http.Server{
		Addr:    endpoint,
		Handler: hs.engine,
	}
	hs.shutdownChan = make(chan interface{}, 1)
	hs.leaderId = hs.id
	hs.addRouter()
	return hs
}

func (hs *HTTPServer) Start() {
	hs.dlog("listening in %v...", hs.endpoint)
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				syscall.SetsockoptInt(syscall.Handle(int(fd)), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
		},
	}
	var err error
	hs.listener, err = lc.Listen(context.Background(), "tcp", hs.endpoint)

	//hs.listener, err = net.Listen("tcp", hs.endpoint)
	if err != nil {
		log.Fatalf("listen: %s\n", err)
	}
	// 启动 HTTP 服务器，监听端口
	go func() {
		if err := hs.server.Serve(hs.listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	<-hs.shutdownChan // 阻塞，直到接收到关闭通知
}

func (hs *HTTPServer) Shutdown() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.dlog("Shutdown Server ...")

	close(hs.shutdownChan) // 发送关闭通知
	//创建超时上下文，Shutdown可以让未处理的连接在这个时间内关闭
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	//停止HTTP服务器
	if err := hs.server.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}

	hs.dlog("Server exiting")
}

func (hs *HTTPServer) addRouter() {
	apiGroup := hs.engine.Group("/api")
	// 定义一个 GET 请求的路由
	apiGroup.GET("/get", hs.handleGet)
	apiGroup.PUT("/put", hs.handlePut)
	apiGroup.DELETE("/del", hs.handleDelete)
}

func (hs *HTTPServer) handleGet(c *gin.Context) {
	// 从路径中获取参数 key
	key := c.Query("key")
	if key == "" {
		response := gin.H{
			"code": -1,
			"msg":  "key should not be empty",
		}
		c.JSON(http.StatusBadRequest, response)
		return
	}
	var reply part8.CommandReply
	err := hs.Get(key, &reply)
	if err != nil {
		hs.dlog("get from hs error:%+v", err)
		response := gin.H{
			"code": -1,
			"msg":  "get key err",
		}
		c.JSON(http.StatusInternalServerError, response)
		return
	}
	// 返回示例 JSON 响应
	response := gin.H{
		"code":  reply.CmdStatus,
		"msg":   reply.CmdStatus.String(),
		"value": reply.Value,
	}
	c.JSON(http.StatusOK, response)
}

type PutArgs struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (hs *HTTPServer) handlePut(c *gin.Context) {
	var putArgs PutArgs
	// 使用 ShouldBindJSON 绑定 JSON 请求体到结构体
	if err := c.ShouldBindJSON(&putArgs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := putArgs.Key
	value := putArgs.Value
	if key == "" || value == "" {
		response := gin.H{
			"code": -1,
			"msg":  "key should not be empty",
		}
		c.JSON(http.StatusBadRequest, response)
		return
	}
	var reply part8.CommandReply
	err := hs.Put(key, value, &reply)
	if err != nil {
		hs.dlog("get from hs error:%+v", err)
		response := gin.H{
			"code": -1,
			"msg":  "get key err",
		}
		c.JSON(http.StatusInternalServerError, response)
		return
	}
	// 返回示例 JSON 响应
	response := gin.H{
		"code":  reply.CmdStatus,
		"msg":   reply.CmdStatus.String(),
		"value": reply.Value,
	}
	c.JSON(http.StatusOK, response)
}

func (hs *HTTPServer) handleDelete(c *gin.Context) {
	// 从路径中获取参数 key
	key := c.Query("key")
	if key == "" {
		response := gin.H{
			"code": -1,
			"msg":  "key should not be empty",
		}
		c.JSON(http.StatusBadRequest, response)
		return
	}
	var reply part8.CommandReply
	err := hs.Delete(key, &reply)
	if err != nil {
		hs.dlog("get from hs error:%+v", err)
		response := gin.H{
			"code": -1,
			"msg":  "get key err",
		}
		c.JSON(http.StatusInternalServerError, response)
		return
	}
	// 返回示例 JSON 响应
	response := gin.H{
		"code":  reply.CmdStatus,
		"msg":   reply.CmdStatus.String(),
		"value": reply.Value,
	}
	c.JSON(http.StatusOK, response)
}

func (hs *HTTPServer) Get(key string, reply *part8.CommandReply) error {
	args := part8.CommandArgs{
		Op:        part8.GET,
		Key:       key,
		CommandId: idGenerator.GenerateID(),
	}
	hs.handleCommand(args, reply)
	return nil
}

func (hs *HTTPServer) Put(key, value string, reply *part8.CommandReply) error {
	args := part8.CommandArgs{
		Op:        part8.PUT,
		Key:       key,
		Value:     value,
		CommandId: idGenerator.GenerateID(),
	}
	hs.handleCommand(args, reply)
	return nil
}

func (hs *HTTPServer) Delete(key string, reply *part8.CommandReply) error {
	args := part8.CommandArgs{
		Op:        part8.DELETE,
		Key:       key,
		CommandId: idGenerator.GenerateID(),
	}
	hs.handleCommand(args, reply)
	return nil
}

//func (hs *HTTPServer) Clear(reply *part8.CommandReply) {
//	args := part8.CommandArgs{
//		Op:        part8.CLEAR,
//		CommandId: idGenerator.GenerateID(),
//	}
//	hs.handleCommand(args, reply)
//}

func (hs *HTTPServer) handleCommand(args part8.CommandArgs, reply *part8.CommandReply) error {
	hs.mu.Lock()
	leaderId := hs.leaderId
	currentId := hs.id
	hs.mu.Unlock()
	savedLeaderId := leaderId
	defer func() {
		if savedLeaderId != leaderId {
			hs.mu.Lock()
			hs.leaderId = leaderId
			hs.mu.Unlock()
		}
	}()
	var err error
	for i := 0; i < 3; i++ {
		//os.Setenv("DebugCM", "true")
		if currentId == leaderId {
			hs.dlog("from from current Node, use leaderId=%v", leaderId)
			err = hs.s.HandleCommand(args, reply)
		} else {
			hs.dlog("rpc from leaderId=%v", leaderId)
			rep := part8.CommandReply{}
			err = hs.s.Call(leaderId, "Command.HandleCommand", args, &rep)
			hs.dlog("rpc reply= %+v", rep)
			*reply = rep
		}
		if err != nil { // 尝试重新建立连接
			hs.s.ReConnectToPeer(leaderId)
		}
		switch reply.CmdStatus {
		case part8.WRONG_LEADER: // 更换leader
			if reply.LeaderId < 0 {
				common.SleepMs(500)
			} else {
				leaderId = reply.LeaderId
			}
			hs.dlog("got wrong leader,reply.leaderId=%v,reply:=%+v", reply.LeaderId, reply)
		case part8.TIMEOUT: // 直接重试
			hs.dlog("timeout,reply:=%v", reply)
		default:
			hs.dlog("get reply:=%v", reply)
			//os.Setenv("DebugCM", "false")
			return nil
		}
	}
	//os.Setenv("DebugCM", "false")
	return err
}

func (hs *HTTPServer) dlog(format string, args ...interface{}) {
	format = fmt.Sprintf("[HTTP Node[%v]] ", hs.id) + format
	log.Printf(format, args...)
}
