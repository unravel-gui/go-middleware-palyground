package part8

import (
	"fmt"
	"math"
)

const NO_LIMIT = math.MaxInt

type RaftLog struct {
	Entries            []LogEntry
	CommitIndex        int
	Applied            int
	MaxNextEntriesSize int
}

func (rl *RaftLog) String() string {
	// 使用 fmt.Sprintf 格式化 RaftLog 的字符串表示
	return fmt.Sprintf("RaftLog{Entries: %+v, CommitIndex: %d, Applied: %d, MaxNextEntriesSize: %d}",
		rl.Entries, rl.CommitIndex, rl.Applied, rl.MaxNextEntriesSize)
}

func NewRaftLog(storage *Storage, maxNextEntriesSize int) *RaftLog {
	if storage == nil {
		return nil
	}
	rs, err := storage.LoadRuntimeState()
	rl := new(RaftLog)
	rl.Applied = 0
	rl.Entries = make([]LogEntry, 0)
	rl.MaxNextEntriesSize = maxNextEntriesSize
	if err != nil || rs == nil || len(rs.Logs) == 0 {
		rl.Entries = append(rl.Entries, LogEntry{Term: 0, Index: 0, Command: nil})
		//rl.Entries = make([]LogEntry, 0)
		rl.CommitIndex = 0
	} else {
		// 存在runtime data
		rl.Entries = rs.Logs
		rl.CommitIndex = rs.CommitIndex
	}

	if maxNextEntriesSize == 0 {
		maxNextEntriesSize = NO_LIMIT
	}
	rl.MaxNextEntriesSize = maxNextEntriesSize
	return rl
}

func (rl *RaftLog) Append(entry LogEntry) {
	rl.Entries = append(rl.Entries, entry)
}

func (rl *RaftLog) size() int {
	return len(rl.Entries)
}
func (rl *RaftLog) firstIndex() int {
	return rl.Entries[0].Index + 1
}

func (rl *RaftLog) lastIndex() int {
	return rl.Entries[len(rl.Entries)-1].Index
}
func (rl *RaftLog) lastTerm() int {
	return rl.Entries[len(rl.Entries)-1].Term
}

func (rl *RaftLog) nextIndex() int {
	return rl.Entries[len(rl.Entries)-1].Index + 1
}

// 调用前保证index存在
func (rl *RaftLog) term(index int) int {
	off := rl.lastSnapshotIndex()
	return rl.Entries[index-off].Term
}

//func (rl *RaftLog) IsUpToDate(index, term int) bool {
//	lastIndex := rl.lastIndex()
//	lastTerm := rl.lastTerm()
//	return term > lastTerm || (term == lastTerm && index >= lastIndex)
//}

func (rl *RaftLog) lastSnapshotIndex() int {
	return rl.Entries[0].Index
}

func (rl *RaftLog) lastSnapshotTerm() int {
	return rl.Entries[0].Term
}

func (rl *RaftLog) getRelIndex(index int) int {
	off := rl.lastSnapshotIndex()
	return index - off
}

// 判断给定的日志和实际日志是否相符
func (rl *RaftLog) matchLog(prevIndex, prevLogTerm int) bool {
	off := rl.lastSnapshotIndex()
	if prevIndex > off {
		return false
	}
	relIndex := prevIndex - off
	return rl.Entries[relIndex].Index == prevIndex && rl.Entries[relIndex].Term == prevLogTerm
}

func (rl *RaftLog) createSnapShot(index int, data []byte) *SnapShot {
	// 不在可持久化的范围内
	if index < rl.firstIndex() || index > rl.CommitIndex || index > rl.lastIndex() {
		return nil
	}
	off := rl.firstIndex()
	snapTerm := rl.term(index - off)
	snap := NewSnapShot(index, snapTerm, data)
	return snap
}

func (rl *RaftLog) compact(compactIndex int) bool {
	off := rl.firstIndex()
	// 没有可压缩的日志,或者超出范围
	if compactIndex <= off || compactIndex > rl.lastIndex() {
		return false
	}
	index := compactIndex - off
	rl.Entries = rl.Entries[:index]
	// 预留一位
	rl.Entries[0].Command = nil
	return true
}

//
//func (rl *RaftLog) maybeAppend(index, term, commitIndex int, entries []LogEntry) int {
//	if rl.matchLog(index, term) {
//		lastIndexOfNewEntries := index + len(entries)
//		conflictIndex := rl.findConflict(entries)
//		if conflictIndex > 0 {
//			if conflictIndex <= rl.CommitIndex {
//				// 冲突索引位置小于提交索引，我们认为已提交的数据是正确的，因此当前节点直接退出
//				os.Exit(-1)
//			} else {
//				off := index + 1
//				// 冲突索引超出可修正范围范围
//				if conflictIndex-off > len(entries) {
//					os.Exit(-1)
//				}
//				rl.Entries = append(rl.Entries[conflictIndex-off:], entries...)
//			}
//		}
//		rl.commitTo(common.MinInt(commitIndex, lastIndexOfNewEntries))
//		return lastIndexOfNewEntries
//	}
//	return -1
//}
//func (rl *RaftLog) findConflict(entries []LogEntry) int {
//	for _, entry := range entries {
//		if !rl.matchLog(entry.Index, entry.Term) { // 找到第一处不同点返回
//			return entry.Index
//		}
//	}
//	return 0
//}
//func (rl *RaftLog) FindConflict(prevLogIndex int, prevLogTerm int) int {
//	last := rl.lastIndex()
//	if prevLogIndex > last {
//		return last + 1
//	}
//	first := rl.firstIndex()
//	conflictTerm := rl.term(prevLogIndex)
//	index := prevLogIndex - 1
//	// 尝试跳过一个term
//	for index >= first && (rl.term(index) == conflictTerm) {
//		index--
//	}
//	return index
//}
//func (rl *RaftLog) matchLog(index, term int) bool {
//	t := rl.term(index)
//	if t < 0 {
//		return false
//	}
//	return t == term
//}
//func (rl *RaftLog) commitTo(commit int) {
//	if rl.CommitIndex < commit {
//		// 超出范围
//		if rl.lastIndex() < commit {
//			return
//		}
//		rl.CommitIndex = commit
//	}
//}
//
//// AppliedTo 更新applied进度 (应用于状态机的进度)
//func (rl *RaftLog) AppliedTo(index int) {
//	if index <= 0 {
//		return
//	}
//	if rl.CommitIndex < index || index > rl.Applied {
//		return
//	}
//	rl.Applied = index
//}
//func (rl *RaftLog) maybeCommit(maxIndex, term int) bool {
//	if maxIndex > rl.CommitIndex && rl.term(maxIndex) == term {
//		rl.commitTo(maxIndex)
//		return true
//	}
//	return false
//}
