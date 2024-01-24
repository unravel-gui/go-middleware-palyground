package part8

import (
	"fmt"
)

type MsgType int

const (
	ENTRY MsgType = iota
	SNAPSHOT
)

type ApplyMsg struct {
	Type    MsgType
	Command []byte
	Index   int
	Term    int
}

func NewApplyMsgFromEntry(ent LogEntry) ApplyMsg {
	return ApplyMsg{
		Type:    ENTRY,
		Command: ent.Command,
		Index:   ent.Index,
		Term:    ent.Term,
	}
}

func NewApplyMsgFromSnapshot(snap *SnapShot) ApplyMsg {
	return ApplyMsg{
		Type:    SNAPSHOT,
		Command: snap.Data,
		Index:   snap.MetaData.Index,
		Term:    snap.MetaData.Term,
	}
}

func (msg ApplyMsg) ToString() string {
	var t string
	switch msg.Type {
	case ENTRY:
		t = "Entry"
	case SNAPSHOT:
		t = "Snapshot"
	default:
		t = "Unexpected"
	}

	return fmt.Sprintf("type: %s, index: %d, term: %d, command: %v", t, msg.Index, msg.Term, msg.Command)
}
