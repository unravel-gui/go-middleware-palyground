package part6

import "fmt"

type MsgType int

const (
	ENTRY MsgType = iota
	SNAPSHOT
)

type ApplyMsg struct {
	Type  MsgType
	Data  []byte
	Index int64
	Term  int64
}

func NewApplyMsgFromEntry(ent LogEntry) ApplyMsg {
	return ApplyMsg{
		Type:  ENTRY,
		Data:  ent.Data,
		Index: ent.Index,
		Term:  ent.Term,
	}
}

func NewApplyMsgFromSnapshot(snap *SnapShot) ApplyMsg {
	return ApplyMsg{
		Type:  SNAPSHOT,
		Data:  snap.Data,
		Index: snap.MetaData.Index,
		Term:  snap.MetaData.Term,
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

	return fmt.Sprintf("type: %s, index: %d, term: %d, data size: %d", t, msg.Index, msg.Term, len(msg.Data))
}
