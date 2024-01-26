package httpServer

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"raft/part8"
	"raft/part8/common"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	// 设置日志内容
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

var basicHost string
var basicPort int
var basicDir string

func init() {
	basicHost = "127.0.0.1"
	basicPort = 9000
	basicDir = "test"
	resetBasicDir(basicDir)
}

func GetEndpoint(p int) string {
	return fmt.Sprintf("%v:%d", basicHost, basicPort+p)
}

func resetBasicDir(filePath string) {
	// 删除目录及其子目录和文件
	err := os.RemoveAll(filePath)
	if err != nil {
		tlog("Error removing directory: %v", err)
	}

	// 创建目录
	err = os.MkdirAll(filePath, 0755)
	if err != nil {
		tlog("Error creating directory: %v", err)
	}
}

type Harness struct {
	mu      sync.Mutex   // 保证高并发数据安全
	cluster []*StartNode // 保存所有节点的服务器对象
	alive   []bool

	dirPath string
	n       int        // 节点数量
	t       *testing.T // 测试 t
}

// NewHarness 新建辅助对象
func NewHarness(t *testing.T, n int) *Harness {
	// 初始化
	ns := make([]*StartNode, n)
	timestamp := time.Now().Format("2006-01-02_15-04-05.000")
	dirPath := fmt.Sprintf("%s/%s", basicDir, timestamp)
	alive := make([]bool, n)
	// 初始化每个节点的内容
	for i := 0; i < n; i++ {
		// 计算当前节点的其他节点列表
		peerIds := make(map[int]string, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds[p] = GetEndpoint(p)
			}
		}
		filePath := fmt.Sprintf("%v/raft-state-%d", dirPath, i)
		ns[i] = NewStartNode(i, i, peerIds, filePath)
		ns[i].Start()
		alive[i] = true
	}
	h := &Harness{
		cluster: ns,
		alive:   alive,
		dirPath: dirPath,
		n:       n,
		t:       t,
	}
	return h
}
func (h *Harness) getLeader() int {
	for {
		for i := 0; i < h.n; i++ {
			if !h.alive[i] {
				continue
			}
			if _, _, isLeader := h.cluster[i].Report(); isLeader {
				tlog("Node[%v] is Leader now", i)
				return i
			}
			//tlog("Node[%v] is %v", i, h.cluster[i].getState())
		}
	}
}
func (h *Harness) Crash(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	//os.Setenv("DebugCM", "true")
	tlog("Node[%v] is crash...", id)
	go h.cluster[id].Shutdown()
	h.alive[id] = false
	common.SleepMs(100)
}

func (h *Harness) Restart(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	//os.Setenv("DebugCM", "true")
	tlog("Node[%v] is restart...", id)
	peerIds := make(map[int]string, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds[p] = GetEndpoint(p)
		}
	}
	filePath := fmt.Sprintf("%s/raft-state-%d", h.dirPath, id)
	h.cluster[id] = NewStartNode(id, id, peerIds, filePath)
	h.cluster[id].Start()
	h.alive[id] = true
}

func (h *Harness) Get(key string) part8.CommandReply {
	leaderId := h.getLeader()
	return h.cluster[leaderId].GetForTest(key)
}

func (h *Harness) Put(key, value string) part8.CommandReply {
	leaderId := h.getLeader()
	return h.cluster[leaderId].PutForTest(key, value)
}

func (h *Harness) GetByHTTP(key string) string {
	leaderId := h.getLeader()
	apiURL := fmt.Sprintf("http://%s/api/get", h.cluster[leaderId].endpoint)
	// 发送 GET 请求
	response, err := http.Get(apiURL + "?key=" + key)
	if err != nil {
		fmt.Println("Error making GET request:", err)
		return ""
	}
	defer response.Body.Close()
	var resp HttpResponse
	if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
		if err != io.EOF {
			fmt.Println("Error decoding JSON:", err)
		}
	}
	return resp.Value
}

func (h *Harness) PutByHTTP(key, value string) bool {
	leaderId := h.getLeader()
	apiURL := fmt.Sprintf("http://%s/api/put", h.cluster[leaderId].endpoint)
	// 发送 Put 请求
	// 构造请求体参数
	payload := strings.NewReader(`{"key": "` + key + `", "value": "` + value + `"}`)
	putReq, err := http.NewRequest("PUT", apiURL, payload)
	if err != nil {
		fmt.Println("Error creating PUT request:", err)
		return false
	}
	putReq.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(putReq)
	if err != nil {
		fmt.Println("Error making PUT request:", err)
		return false
	}
	defer response.Body.Close()
	var resp HttpResponse
	if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
		if err != io.EOF {
			fmt.Println("Error decoding JSON:", err)
			return false
		}
	}
	return resp.Msg == part8.OK.String()
}

func (h *Harness) DelByHTTP(key string) bool {
	leaderId := h.getLeader()
	apiURL := fmt.Sprintf("http://%s/api/del?key=%v", h.cluster[leaderId].endpoint, key)
	deleteReq, err := http.NewRequest("DELETE", apiURL, nil)
	if err != nil {
		fmt.Println("Error creating DELETE request:", err)
		return false
	}
	deleteReq.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		fmt.Println("Error making DELETE request:", err)
		return false
	}
	defer response.Body.Close()
	var resp HttpResponse
	if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
		if err != io.EOF {
			fmt.Println("Error decoding JSON:", err)
			return false
		}
	}
	return resp.Msg == part8.OK.String()
}

// Shutdown 关闭辅助对象
func (h *Harness) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	//os.Setenv("DebugCM", "true")
	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			go h.cluster[i].Shutdown()
		}
	}
}

// 封装打印日志
func tlog(format string, a ...interface{}) {
	format = "[TEST]" + format
	log.Printf(format, a...)
}

type HttpResponse struct {
	Code  int    `json:"code"`
	Msg   string `json:"msg"`
	Value string `json:"value"`
}
