package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
)

type OpType int

const (
	INSERT_FILE OpType = iota
	MODIFY_FILE
	DELETE_FILE
	QUERY_FILE
)

func (op OpType) String() string {
	switch op {
	case INSERT_FILE:
		return "INSERT_FILE"
	case MODIFY_FILE:
		return "MODIFY_FILE"
	case DELETE_FILE:
		return "DELETE_FILE"
	case QUERY_FILE:
		return "QUERY_FILE"
	default:
		return "Unknown OpType"
	}
}
func StringToOpType(s string) (OpType, error) {
	switch s {
	case "INSERT_FILE":
		return INSERT_FILE, nil
	case "MODIFY_FILE":
		return MODIFY_FILE, nil
	case "DELETE_FILE":
		return DELETE_FILE, nil
	case "QUERY_FILE":
		return QUERY_FILE, nil
	default:
		return -1, fmt.Errorf("unknown OpType: %s", s)
	}
}

type Command struct {
	OpType
	Params *FileMeta
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{OpType=%v,Params=%+v}", cmd.OpType, cmd.Params.String())
}

type HandlerMap map[string]Handler

type Handler interface {
	Execute(command Command) (interface{}, error)
}

type CommandProcessor struct {
	handlerMap HandlerMap
}

func init() {
	gob.Register(Command{})
}

func NewCommandProcessor(cm *ConsensusModule) *CommandProcessor {
	cp := new(CommandProcessor)
	cp.handlerMap = make(HandlerMap)
	insertHandler := &InsertFileMetaHandler{cm: cm}
	registerHandler(cp.handlerMap, INSERT_FILE.String(), insertHandler)
	modifyHandler := &ModifyFileMetaHandler{cm: cm}
	registerHandler(cp.handlerMap, MODIFY_FILE.String(), modifyHandler)
	deleteHandler := &DeleteFileMetaHandler{cm: cm}
	registerHandler(cp.handlerMap, DELETE_FILE.String(), deleteHandler)
	queryHandler := &QueryFileMetaHandler{cm: cm}
	registerHandler(cp.handlerMap, QUERY_FILE.String(), queryHandler)
	return cp
}

func (cp *CommandProcessor) Process(command Command) (interface{}, error) {
	handler, ok := cp.handlerMap[command.OpType.String()]
	if !ok {
		errMsg := fmt.Sprintf("handler is not exist,command=%v", command)
		dlog.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return handler.Execute(command)
}

func registerHandler(hm HandlerMap, handlerType string, handler Handler) {
	hm[handlerType] = handler
}

type InsertFileMetaHandler struct {
	cm *ConsensusModule
}

func (insertHandler *InsertFileMetaHandler) Execute(command Command) (interface{}, error) {
	if command.OpType != INSERT_FILE {
		return nil, errors.New(fmt.Sprintf("command is not insert,is %v", command.OpType))
	}
	//if fileMeta, ok := command.Params.(*FileMeta); ok {
	if fileMeta := command.Params; fileMeta != nil {
		insertHandler.cm.FileMap.Upsert(fileMeta)
		return nil, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Not a FileMeta type, fm= %+v", command.Params))
	}
}

type ModifyFileMetaHandler struct {
	cm *ConsensusModule
}

func (modifyHandler *ModifyFileMetaHandler) Execute(command Command) (interface{}, error) {
	if command.OpType != MODIFY_FILE {
		return nil, errors.New(fmt.Sprintf("command is not modify,is %v", command.OpType))
	}
	//if fileMeta, ok := command.Params.(*FileMeta); ok {
	if fileMeta := command.Params; fileMeta != nil {
		modifyHandler.cm.FileMap.Upsert(fileMeta)
		return nil, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Not a FileMeta type, fm= %+v", command.Params))
	}
}

type DeleteFileMetaHandler struct {
	cm *ConsensusModule
}

func (deleteHandler *DeleteFileMetaHandler) Execute(command Command) (interface{}, error) {
	if command.OpType != DELETE_FILE {
		return nil, errors.New(fmt.Sprintf("command is not delete,is %v", command.OpType))
	}
	//if fileMeta, ok := command.Params.(*FileMeta); ok {
	if fileMeta := command.Params; fileMeta != nil {
		deleteHandler.cm.FileMap.Delete(fileMeta.FileHash)
		return nil, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Not a FileMeta type, fm= %+v", command.Params))
	}
}

type QueryFileMetaHandler struct {
	cm *ConsensusModule
}

func (queryHandler *QueryFileMetaHandler) Execute(command Command) (interface{}, error) {
	if command.OpType != QUERY_FILE {
		return nil, errors.New(fmt.Sprintf("command is not query,is %v", command.OpType))
	}
	//if fileMeta, ok := command.Params.(*FileMeta); ok {
	if fileMeta := command.Params; fileMeta != nil {
		fileMata, _ := queryHandler.cm.FileMap.Get(fileMeta.FileHash)
		return fileMata, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Not a FileMeta type, fm= %+v", command.Params))
	}
}
