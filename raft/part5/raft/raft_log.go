package raft

import (
	"kvRaft/common"
	"math"
	"os"
)

const NO_LIMIT = math.MaxInt64

type RaftLog struct {
	Entries            []LogEntry
	CommitIndex        int64
	Applied            int64
	MaxNextEntriesSize int64
}

func NewRaftLog(persist *Persister, maxNextEntriesSize int64) *RaftLog {
	if persist == nil {
		dlog.Error("persist must not be nil")
		return nil
	}
	raftLogs := persist.LoadEntries()
	rl := new(RaftLog)
	if raftLogs == nil || len(raftLogs) == 0 {
		rl.Entries = []LogEntry{{Term: -1, Index: -1, Data: nil}}
		rl.CommitIndex = -1
		rl.Applied = -1
	} else {
		// 存在runtime data
		rl.Entries = raftLogs
		rl.CommitIndex = persist.LoadRuntimeState().CommitIndex
		// TODO: 确定applied
		rl.Applied = persist.LoadRuntimeState().CommitIndex
	}

	if maxNextEntriesSize == 0 {
		maxNextEntriesSize = NO_LIMIT
	}
	rl.MaxNextEntriesSize = maxNextEntriesSize
	return rl
}

func (rl *RaftLog) allEntries() []LogEntry {
	return rl.Entries
}

func (rl *RaftLog) CreateSnapShot(index int64, data []byte) *SnapShot {
	// 请求建立快照部分日志已经被压缩
	firstIndex := rl.firstIndex()
	lastIndex := rl.lastIndex()
	if index <= firstIndex || index > lastIndex {
		dlog.Error("snapShot %v out of bound index:first(%v) to last(%v) ", index, firstIndex, lastIndex)
		return nil
	}
	// 超出提交进度
	if index > rl.CommitIndex {
		dlog.Error("index > commitIndex")
		return nil
	}
	snap := NewSnapShot(index, rl.term(lastIndex-firstIndex), data)
	dlog.Info("createSnapShot index=%v,snapShot=%+v", index, snap)
	return snap
}

// Compact 压缩日志,压缩到compactIndex
func (rl *RaftLog) Compact(compactIndex int64) bool {
	firstIndex := rl.firstIndex()
	lastIndex := rl.lastIndex()
	// 压缩日志超出范围
	if compactIndex <= firstIndex || compactIndex > lastIndex {
		dlog.Error("compactIndex %v out of bound index:first(%v) to last(%v) ", compactIndex, firstIndex, lastIndex)
		return false
	}
	// 压缩，实际为丢弃，compactIndex-firstIndex位置上的代码已经被执行
	rl.Entries = rl.Entries[:compactIndex-firstIndex]
	// 压缩后第0条数据改为空方便后续寻找index和term,不影响数据准确性
	rl.Entries[0].Data = nil
	return true
}

func (rl *RaftLog) firstIndex() int64 {
	if len(rl.Entries) > 0 {
		return rl.Entries[0].Index
	}
	return math.MaxInt64
}

func (rl *RaftLog) lastIndex() int64 {
	if len(rl.Entries) > 0 {
		return rl.Entries[len(rl.Entries)-1].Index
	}
	return -1
}
func (rl *RaftLog) lastTerm() int64 {
	if len(rl.Entries) > 0 {
		return rl.Entries[len(rl.Entries)-1].Term
	}
	return -1
}

// 调用前保证index存在
func (rl *RaftLog) term(index int64) int64 {
	firstIndex := rl.Entries[0].Index
	// 超出范围
	if index < firstIndex || index-firstIndex >= int64(len(rl.Entries)) {
		return -1
	}
	return rl.Entries[index-firstIndex].Term
}

func (rl *RaftLog) IsUpToDate(index, term int64) bool {
	lastIndex := rl.lastIndex()
	lastTerm := rl.lastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rl *RaftLog) lastSnapshotIndex() int64 {
	return rl.Entries[0].Index
}

func (rl *RaftLog) lastSnapshotTerm() int64 {
	return rl.Entries[0].Term
}

func (rl *RaftLog) maybeCommit(maxIndex, term int64) bool {
	if maxIndex > rl.CommitIndex && rl.term(maxIndex) == term {
		rl.commitTo(maxIndex)
		return true
	}
	return false
}

func (rl *RaftLog) commitTo(commit int64) {
	if rl.CommitIndex < commit {
		// 超出范围
		if rl.lastIndex() < commit {
			dlog.Error("commit=%v is out of range lastIndex=%v", commit, rl.lastIndex())
			return
		}
		rl.CommitIndex = commit
	}
}

func (rl *RaftLog) maybeAppend(pervLogIndex, pervLogTerm, commitIndex int64, entries []LogEntry) int64 {
	if rl.matchLog(pervLogIndex, pervLogTerm) {
		lastIndexOfNewEntries := pervLogIndex + int64(len(entries))
		conflictIndex := rl.findConflict(entries)
		if conflictIndex != 0 {
			if conflictIndex <= rl.CommitIndex {
				dlog.Error("entry %v conflict with committed entry %v", conflictIndex, rl.CommitIndex)
				os.Exit(-1)
			} else {
				off := pervLogIndex + 1
				if conflictIndex-off > int64(len(entries)) {
					dlog.Error("index %v is out of range len=%v", conflictIndex-off, len(entries))
					os.Exit(-1)
				}
				// 添加日志
				rl.Entries = append(rl.Entries[conflictIndex-off:], entries...)

			}
		}
		// 更新commit
		rl.commitTo(common.MinInt64(commitIndex, lastIndexOfNewEntries))
		return lastIndexOfNewEntries
	}
	return -1
}

// 日志是否相同
func (rl *RaftLog) matchLog(index, term int64) bool {
	t := rl.term(index)
	if t < 0 {
		return false
	}
	return t == term
}

func (rl *RaftLog) findConflict(entries []LogEntry) int64 {
	for _, entry := range entries {
		if !rl.matchLog(entry.Index, entry.Term) { // 找到第一处不同点返回
			if entry.Index <= rl.lastIndex() {
				dlog.Error("found conflict at index %v, existing term:%v,conflict term:%v", entry.Index, rl.term(entry.Index), entry.Term)
			}
			return entry.Index
		}
	}
	return 0
}

func (rl *RaftLog) FindConflict(prevLogIndex int64) int64 {
	last := rl.lastIndex()
	if prevLogIndex > last { // 数据丢失
		dlog.Error("index %v is out of range [0,%v] in conflict", prevLogIndex, last)
		return last + 1
	}
	first := rl.firstIndex()
	conflictTerm := rl.term(prevLogIndex)
	index := prevLogIndex - 1
	// 尝试跳过一个term
	for index >= first && (rl.term(index) == conflictTerm) {
		index--
	}
	return index
}

func (rl *RaftLog) ClearEntries(lastSnapShotIndex, lastSnapShotTerm int64) {
	rl.Entries = rl.Entries[:0]
	rl.Entries = append(rl.Entries, LogEntry{
		Term:  lastSnapShotTerm,
		Index: lastSnapShotIndex,
	})
}

func (rl *RaftLog) Append(entry LogEntry) {
	rl.Entries = append(rl.Entries, entry)
}

func (rl *RaftLog) NextEntries() []LogEntry {
	off := common.MaxInt64(rl.Applied+1, rl.firstIndex())
	if rl.CommitIndex+1 > off {
		return rl.RaftLogSlice(off, rl.CommitIndex, rl.MaxNextEntriesSize)
	}
	return nil
}

func (rl *RaftLog) RaftLogSlice(low, high, maxSize int64) []LogEntry {
	rl.mustCheckOutOfBounds(low, high-1)
	if maxSize != NO_LIMIT {
		high = common.MinInt64(high, low+maxSize)
	}
	// 计算实际下标
	low -= rl.lastSnapshotIndex()
	high -= rl.lastSnapshotIndex()

	return rl.Entries[low:high]
}

// mustCheckOutOfBounds 检查下标范围是否越界
func (rl *RaftLog) mustCheckOutOfBounds(low, high int64) {
	if low > high {
		dlog.Error("invalid slice %v>%v", low, high)
		os.Exit(-1)
	}
	if low < rl.firstIndex() || high > rl.lastIndex() {
		dlog.Error("invalid slice [%v,%v] out of bound [%v,%v]", low, high, rl.firstIndex(), rl.lastIndex())
		os.Exit(-1)
	}
}

// AppliedTo 更新applied进度 (应用于状态机的进度)
func (rl *RaftLog) AppliedTo(index int64) {
	if index <= 0 {
		return
	}
	if rl.CommitIndex < index || index > rl.Applied {
		dlog.Error("applied %v is out of range prevApplied=%v, commitIndex=%v", index, rl.Applied, rl.CommitIndex)
		return
	}
	rl.Applied = index
}
