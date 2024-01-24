package part8

type Operation int

const (
	GET Operation = iota
	PUT
	DELETE
	CLEAR
)

type CmdStatus int

const (
	OK CmdStatus = iota
	NO_KEY
	WRONG_LEADER
	TIMEOUT
	CLOSED
)

func (err CmdStatus) String() string {
	var str string
	switch err {
	case OK:
		str = "OK"
	case NO_KEY:
		str = "No Key"
	case WRONG_LEADER:
		str = "Wrong Leader"
	case TIMEOUT:
		str = "Timeout"
	case CLOSED:
		str = "Closed"
	default:
		str = "Unexpect CmdStatus"
	}
	return str
}

// String 实现了类似的功能
func (op Operation) String() string {
	var str string
	switch op {
	case GET:
		str = "GET"
	case PUT:
		str = "PUT"
	case DELETE:
		str = "DELETE"
	case CLEAR:
		str = "CLEAR"
	default:
		str = "Unexpect Operation"
	}
	return str
}

type CommandArgs struct {
	Op        Operation
	Key       string
	Value     string
	ClientId  int
	CommandId int
}

type CommandReply struct {
	CmdStatus
	Value    string
	LeaderId int
}
