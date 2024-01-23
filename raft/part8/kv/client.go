package kv

//
//type KVClient struct {
//	mu        sync.Mutex
//	peers     map[int]string
//	clientId  int
//	leaderId  int
//	commandId int
//	stop      common.AtomicBool
//	quit      chan interface{}
//}
//
//func (kv *KVClient) Command(args CommandArgs, reply *CommandReply) error {
//	args.ClientId = kv.clientId
//	args.CommandId = kv.commandId
//	for !kv.stop.Get() {
//		// 连接rpc服务端
//
//	}
//}
