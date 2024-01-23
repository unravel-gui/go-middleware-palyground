package raft

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

type SnapShotMetaData struct {
	Index int64
	Term  int64
}

func NewSnapShotMetaData(index, term int64) SnapShotMetaData {
	return SnapShotMetaData{
		Index: index,
		Term:  term,
	}
}

type SnapShot struct {
	MetaData SnapShotMetaData
	Data     []byte
}

func NewSnapShot(index, term int64, data []byte) *SnapShot {
	return &SnapShot{
		MetaData: NewSnapShotMetaData(index, term),
		Data:     data,
	}
}

func (s *SnapShot) empty() bool {
	return s.MetaData.Index == 0
}

type SnapShotManager struct {
	Dir        string
	SnapSuffix string
}

func NewSnapShotManager(path string, args ...string) *SnapShotManager {
	suffix := ".snap"
	if len(args) != 0 {
		suffix = args[0]
	}
	return &SnapShotManager{
		Dir:        path,
		SnapSuffix: suffix,
	}
}

// SaveSnap 存储并持久化快照
func (s *SnapShotManager) SaveSnap(shot *SnapShot) bool {
	if shot == nil || shot.empty() {
		return false
	}
	return s.save(shot) == nil
}

// LoadSnap 加载最新的快照
func (s *SnapShotManager) LoadSnap() *SnapShot {
	snapNames := s.snapNames()
	if len(snapNames) < 0 {
		return nil
	}

	for _, snapName := range snapNames {
		snap, err := s.read(snapName)
		if err == nil && snap != nil {
			return snap
		}
	}
	return nil
}

// 按照新到旧的顺序返回快照列表
func (s *SnapShotManager) snapNames() (snaps []string) {
	info, err := os.Stat(s.Dir)
	if err != nil {
		return
	}
	if !info.IsDir() {
		return
	}

	// 读取目录下的文件列表
	files, err := ioutil.ReadDir(s.Dir)
	if err != nil {
		return
	}

	for _, file := range files {
		// 忽略非常规文件
		if file.Mode().IsRegular() {
			filename := file.Name()
			// 检查文件后缀
			if strings.HasSuffix(filename, s.SnapSuffix) {
				snaps = append(snaps, filename)
			} else {
				fmt.Printf("Skipped unexpected non-snapshot file: %s\n", filename)
			}
		}
	}
	// 对文件名进行排序
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i] > snaps[j]
	})

	return snaps
}

// 检查文件后缀
func (s *SnapShotManager) checkSuffix(snapNames []string) []string {
	var snaps []string
	for _, snapName := range snapNames {
		if strings.HasSuffix(snapName, s.SnapSuffix) {
			snaps = append(snaps, snapName)
		} else {
			fmt.Printf("skipped unexpected non snapshot file %s\n", snapName)
		}
	}
	return snaps
}

// 序列化快照后持久化到磁盘
func (s *SnapShotManager) save(snapShot *SnapShot) error {
	// 快照名格式 %016d-%016d%s
	filename := fmt.Sprintf("%016d-%016d%s", snapShot.MetaData.Term, snapShot.MetaData.Index, s.SnapSuffix)
	filePath := filepath.Join(s.Dir, filename)
	file, err := os.Create(filePath)
	if err != nil {
		dlog.Error("save snap shot err:%v", err)
		return err
	}
	defer file.Close()

	// 使用gob进行序列化
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(snapShot)
	if err != nil {
		return err
	}

	// 刷盘
	err = file.Sync()
	if err != nil {
		return err
	}
	return nil
}

// 读取快照
func (s *SnapShotManager) read(snapName string) (*SnapShot, error) {
	filePath := path.Join(s.Dir, snapName)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var snapShot *SnapShot
	if err = gob.NewDecoder(file).Decode(&snapShot); err != nil {
		return nil, err
	}
	return snapShot, nil
}
