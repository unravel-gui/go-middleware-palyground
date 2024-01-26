package httpServer

import (
	"encoding/json"
	"fmt"
	"github.com/fortytw2/leaktest"
	"io"
	"log"
	"net/http"
	"raft/part8"
	"strings"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()
	leaderId := h.getLeader()
	peerId := (leaderId + 1) % h.n
	notExistKey := "not_exist_key"
	key := "key"
	value := "success"
	h.Put(key, value)
	tests := []struct {
		testName string
		run      func(t *testing.T)
	}{
		{
			"get not exist key to peer",
			func(t *testing.T) {
				// 创建一个模拟的 HTTP GET 请求
				apiURL := fmt.Sprintf("http://%s/api/get", h.cluster[peerId].endpoint)
				// 发送 GET 请求
				response, err := http.Get(apiURL + "?key=" + notExistKey)
				if err != nil {
					fmt.Println("Error making GET request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("get err, want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.NO_KEY {
					t.Fatalf("get err, Code want %v but got %v", part8.NO_KEY, resp.Code)
				}
				if resp.Msg != part8.NO_KEY.String() {
					t.Fatalf("get err, msg want %v but got %v", part8.NO_KEY.String(), resp.Msg)
				}
			},
		},
		{
			"get exist key to peer",
			func(t *testing.T) {
				// 创建一个模拟的 HTTP GET 请求
				apiURL := fmt.Sprintf("http://%s/api/get", h.cluster[peerId].endpoint)
				// 发送 GET 请求
				response, err := http.Get(apiURL + "?key=" + key)
				if err != nil {
					fmt.Println("Error making GET request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("get err, want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.OK {
					t.Fatalf("get err, Code want %v but got %v", part8.OK, resp.Code)
				}
				if resp.Msg != part8.OK.String() {
					t.Fatalf("get err, want %v but got %v", part8.OK.String(), resp.Msg)
				}
				if resp.Value != value {
					t.Fatalf("get err, value want %v but got %v", value, resp.Value)
				}
			},
		},
		{
			"get not exist key to leader",
			func(t *testing.T) {
				// 创建一个模拟的 HTTP GET 请求
				apiURL := fmt.Sprintf("http://%s/api/get", h.cluster[leaderId].endpoint)
				// 发送 GET 请求
				response, err := http.Get(apiURL + "?key=" + notExistKey)
				if err != nil {
					fmt.Println("Error making GET request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("get err, want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.NO_KEY {
					t.Fatalf("get err, Code want %v but got %v", part8.NO_KEY, resp.Code)
				}
				if resp.Msg != part8.NO_KEY.String() {
					t.Fatalf("get err, msg want %v but got %v", part8.NO_KEY.String(), resp.Msg)
				}
			},
		},
		{
			"get exist key to leader",
			func(t *testing.T) {
				// 创建一个模拟的 HTTP GET 请求
				apiURL := fmt.Sprintf("http://%s/api/get", h.cluster[leaderId].endpoint)
				// 发送 GET 请求
				response, err := http.Get(apiURL + "?key=" + key)
				if err != nil {
					fmt.Println("Error making GET request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("get err, want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.OK {
					t.Fatalf("get err, Code want %v but got %v", part8.OK, resp.Code)
				}
				if resp.Msg != part8.OK.String() {
					t.Fatalf("get err, msg want %v but got %v", part8.OK.String(), resp.Msg)
				}
				if resp.Value != value {
					t.Fatalf("get err, value want %v but got %v", value, resp.Value)
				}
			},
		},
	}

	for _, test := range tests {
		test.run(t)
	}
}

func TestPut(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	leaderId := h.getLeader()
	peerId := (leaderId + 1) % h.n
	keyToLeader := "not_exist_key_leader"
	keyToPeer := "not_exist_key_peer"
	value := "success"
	updateValue := "success!!!"
	tests := []struct {
		testName string
		run      func(t *testing.T)
	}{
		{
			"put key to peer",
			func(t *testing.T) {
				apiURL := fmt.Sprintf("http://%s/api/put", h.cluster[peerId].endpoint)
				// 发送 Put 请求
				// 构造请求体参数
				payload := strings.NewReader(`{"key": "` + keyToPeer + `", "value": "` + value + `"}`)
				putReq, err := http.NewRequest("PUT", apiURL, payload)
				if err != nil {
					fmt.Println("Error creating PUT request:", err)
					return
				}
				putReq.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(putReq)
				if err != nil {
					fmt.Println("Error making PUT request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("put err,want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.OK {
					t.Fatalf("put err,code want 0 but got %v,reply=%+v", resp.Code, resp)
				}
				if resp.Msg != part8.OK.String() {
					t.Fatalf("put err,msg want %v but got %v", part8.OK.String(), resp.Msg)
				}
				reply := h.Get(keyToPeer)
				if reply.CmdStatus != part8.OK {
					t.Fatalf("put err, get reply= got %+v, put resp=%+v", reply, resp)
				}
				if reply.Value != value {
					t.Fatalf("put err,value want %v but got %v", value, resp.Value)
				}
			},
		},
		{
			"put duplicate key to peer",
			func(t *testing.T) {
				apiURL := fmt.Sprintf("http://%s/api/put", h.cluster[peerId].endpoint)
				// 发送 Put 请求
				// 构造请求体参数
				payload := strings.NewReader(`{"key": "` + keyToPeer + `", "value": "` + updateValue + `"}`)
				putReq, err := http.NewRequest("PUT", apiURL, payload)
				if err != nil {
					fmt.Println("Error creating PUT request:", err)
					return
				}
				putReq.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(putReq)
				if err != nil {
					fmt.Println("Error making PUT request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("put err,want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.OK {
					t.Fatalf("put err,Code want %v but got %v", part8.OK, resp.Code)
				}
				if resp.Msg != part8.OK.String() {
					t.Fatalf("put err,msg want %v but got %v", part8.OK.String(), resp.Msg)
				}
				reply := h.Get(keyToPeer)
				if reply.CmdStatus != part8.OK {
					t.Fatalf("put err, get reply= got %+v, put resp=%+v", reply, resp)
				}
				if reply.Value != updateValue {
					t.Fatalf("put err,value want %v but got %v", updateValue, resp.Value)
				}
			},
		},
		{
			"put key to leader",
			func(t *testing.T) {
				apiURL := fmt.Sprintf("http://%s/api/put", h.cluster[leaderId].endpoint)
				// 发送 Put 请求
				// 构造请求体参数
				payload := strings.NewReader(`{"key": "` + keyToLeader + `", "value": "` + value + `"}`)
				putReq, err := http.NewRequest("PUT", apiURL, payload)
				if err != nil {
					fmt.Println("Error creating PUT request:", err)
					return
				}
				putReq.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(putReq)
				if err != nil {
					fmt.Println("Error making PUT request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("put err,want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.OK {
					t.Fatalf("put err,Code want %v but got %v", part8.OK, resp.Code)
				}
				if resp.Msg != part8.OK.String() {
					t.Fatalf("put err,msg want %v but got %v", part8.OK.String(), resp.Msg)
				}
				reply := h.Get(keyToLeader)
				if reply.CmdStatus != part8.OK {
					t.Fatalf("put err, get reply= got %+v, put resp=%+v", reply, resp)
				}
				if reply.Value != value {
					t.Fatalf("put err,value want %v but got %v", value, resp.Value)
				}
			},
		},
		{
			"put duplicate key to peer",
			func(t *testing.T) {
				apiURL := fmt.Sprintf("http://%s/api/put", h.cluster[leaderId].endpoint)
				// 发送 Put 请求
				// 构造请求体参数
				payload := strings.NewReader(`{"key": "` + keyToLeader + `", "value": "` + updateValue + `"}`)
				putReq, err := http.NewRequest("PUT", apiURL, payload)
				if err != nil {
					fmt.Println("Error creating PUT request:", err)
					return
				}
				putReq.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(putReq)
				if err != nil {
					fmt.Println("Error making PUT request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("put err,want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.OK {
					t.Fatalf("put err,Code want %v but got %v", part8.OK, resp.Code)
				}
				if resp.Msg != part8.OK.String() {
					t.Fatalf("put err,msg want %v but got %v", part8.OK.String(), resp.Msg)
				}
				reply := h.Get(keyToLeader)
				if reply.CmdStatus != part8.OK {
					t.Fatalf("put err, get reply= got %+v, put resp=%+v", reply, resp)
				}
				if reply.Value != updateValue {
					t.Fatalf("put err,value want %v but got %v", updateValue, resp.Value)
				}
			},
		},
	}

	for _, test := range tests {
		test.run(t)
	}
}

func TestDel(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()
	leaderId := h.getLeader()
	peerId := (leaderId + 1) % h.n
	keyLeader := "key_leader"
	keyPeer := "key_peer"
	value := "success"
	h.Put(keyLeader, value)
	h.Put(keyPeer, value)
	tests := []struct {
		testName string
		run      func(t *testing.T)
	}{
		{
			"del exist key to peer",
			func(t *testing.T) {
				apiURL := fmt.Sprintf("http://%s/api/del?key=%v", h.cluster[peerId].endpoint, keyPeer)
				deleteReq, err := http.NewRequest("DELETE", apiURL, nil)
				if err != nil {
					fmt.Println("Error creating DELETE request:", err)
					return
				}
				deleteReq.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(deleteReq)
				if err != nil {
					fmt.Println("Error making DELETE request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}

				if response.StatusCode != 200 {
					t.Fatalf("delete err, want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.OK {
					t.Fatalf("delete err, Code want %v but got %v", part8.OK, resp.Code)
				}
				if resp.Msg != part8.OK.String() {
					t.Fatalf("delete err, msg want %v but got %v", part8.OK.String(), resp.Msg)
				}

				reply := h.Get(keyPeer)
				if reply.CmdStatus != part8.NO_KEY {
					t.Fatalf("delete err, get reply= got %+v, delete resp=%+v", reply, resp)
				}
			},
		},
		{
			"del not exist key to peer",
			func(t *testing.T) {
				apiURL := fmt.Sprintf("http://%s/api/del?key=%v", h.cluster[peerId].endpoint, keyPeer)
				deleteReq, err := http.NewRequest("DELETE", apiURL, nil)
				if err != nil {
					fmt.Println("Error creating DELETE request:", err)
					return
				}
				deleteReq.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(deleteReq)
				if err != nil {
					fmt.Println("Error making DELETE request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("delete err, want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.NO_KEY {
					t.Fatalf("delete err, Code want %v but got %v", part8.NO_KEY, resp.Code)
				}
				if resp.Msg != part8.NO_KEY.String() {
					t.Fatalf("delete err, msg want %v but got %v", part8.NO_KEY.String(), resp.Msg)
				}
			},
		},
		{
			"del exist key to leader",
			func(t *testing.T) {
				apiURL := fmt.Sprintf("http://%s/api/del?key=%v", h.cluster[leaderId].endpoint, keyLeader)
				deleteReq, err := http.NewRequest("DELETE", apiURL, nil)
				if err != nil {
					fmt.Println("Error creating DELETE request:", err)
					return
				}
				deleteReq.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(deleteReq)
				if err != nil {
					fmt.Println("Error making DELETE request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}

				if response.StatusCode != 200 {
					t.Fatalf("delete err, want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.OK {
					t.Fatalf("delete err, Code want %v but got %v", part8.OK, resp.Code)
				}
				if resp.Msg != part8.OK.String() {
					t.Fatalf("delete err, msg want %v but got %v", part8.OK.String(), resp.Msg)
				}

				reply := h.Get(keyPeer)
				if reply.CmdStatus != part8.NO_KEY {
					t.Fatalf("delete err, get reply= got %+v, delete resp=%+v", reply, resp)
				}
			},
		},
		{
			"del not exist key to leader",
			func(t *testing.T) {
				apiURL := fmt.Sprintf("http://%s/api/del?key=%v", h.cluster[leaderId].endpoint, keyLeader)
				deleteReq, err := http.NewRequest("DELETE", apiURL, nil)
				if err != nil {
					fmt.Println("Error creating DELETE request:", err)
					return
				}
				deleteReq.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(deleteReq)
				if err != nil {
					fmt.Println("Error making DELETE request:", err)
					return
				}
				defer response.Body.Close()
				var resp HttpResponse
				if err = json.NewDecoder(response.Body).Decode(&resp); err != nil {
					if err != io.EOF {
						fmt.Println("Error decoding JSON:", err)
					}
				}
				fmt.Printf("reply=%+v\n", resp)
				if response.StatusCode != 200 {
					t.Fatalf("delete err, want 200 but got %v", response.StatusCode)
				}
				if part8.CmdStatus(resp.Code) != part8.NO_KEY {
					t.Fatalf("delete err, Code want %v but got %v", part8.NO_KEY, resp.Code)
				}
				if resp.Msg != part8.NO_KEY.String() {
					t.Fatalf("delete err, msg want %v but got %v", part8.NO_KEY.String(), resp.Msg)
				}
			},
		},
	}

	for _, test := range tests {
		test.run(t)
	}
}

func TestLeaderCrashAndRestart(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	key := "key1"
	value := "success"
	_ = h.getLeader()
	leaderId := h.getLeader()
	log.Printf("Leader is Node[%v]\n", leaderId)
	if ok := h.PutByHTTP(key, value); !ok {
		t.Fatalf("Put function is ERROR")
	}
	if v := h.GetByHTTP(key); v != value {
		t.Fatalf("Get function is ERROR")
	}
	if ok := h.DelByHTTP(key); !ok || h.Get(key).Value != "" {
		t.Fatalf("Del function is ERROR")
	}
	h.Crash(leaderId)
	newLeaderId := h.getLeader()
	log.Printf("newLeader is Node[%v]\n", newLeaderId)
	if ok := h.PutByHTTP(key, value); !ok {
		t.Fatalf("Put function is ERROR")
	}
	if v := h.GetByHTTP(key); v != value {
		t.Fatalf("Get function is ERROR")
	}
	if ok := h.DelByHTTP(key); !ok || h.Get(key).Value != "" {
		t.Fatalf("Del function is ERROR")
	}

	h.Restart(leaderId)
	finalLeaderId := h.getLeader()
	log.Printf("finalLeaderId is Node[%v]\n", finalLeaderId)
	if ok := h.PutByHTTP(key, value); !ok {
		t.Fatalf("Put function is ERROR")
	}
	if v := h.GetByHTTP(key); v != value {
		t.Fatalf("Get function is ERROR")
	}
	if ok := h.DelByHTTP(key); !ok || h.Get(key).Value != "" {
		t.Fatalf("Del function is ERROR")
	}

}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
