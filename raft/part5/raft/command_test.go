package raft

import "testing"

func TestInsertFileMetaHandler_Execute(t *testing.T) {
	cm := &ConsensusModule{FileMap: NewFileMetadataStore()}
	cp := NewCommandProcessor(cm)

	// 准备测试数据
	fileMeta := &FileMeta{
		FileHash: "test_hash",
		Size:     1024,
		Path:     []string{"path", "to", "file"},
		Ref:      0,
	}
	command := Command{
		OpType: INSERT_FILE,
		Params: fileMeta,
	}

	_, err := cp.Process(command)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	storedFileMeta, ok := cm.FileMap.Get(fileMeta.FileHash)
	if !ok {
		t.Error("FileMetadata not found in FileMetadataStore")
	} else {
		if storedFileMeta.Ref != 1 {
			t.Errorf("Incorrect reference count, expected 1, got %d", storedFileMeta.Ref)
		}
	}
}

func TestModifyFileMetaHandler_Execute(t *testing.T) {
	cm := &ConsensusModule{FileMap: NewFileMetadataStore()}
	cp := NewCommandProcessor(cm)

	// 准备测试数据
	originalFileMeta := &FileMeta{
		FileHash: "test_hash",
		Size:     1024,
		Path:     []string{"path", "to", "file"},
		Ref:      0,
	}
	insertCommand := Command{
		OpType: INSERT_FILE,
		Params: originalFileMeta,
	}
	_, err := cp.Process(insertCommand)
	if err != nil {
		t.Fatalf("Unexpected error during insert: %v", err)
	}

	// 修改测试数据
	modifiedFileMeta := &FileMeta{
		FileHash: "test_hash",
		Size:     2048,                        // 修改文件大小
		Path:     []string{"updated", "path"}, // 修改文件路径
		Ref:      0,
	}
	modifyCommand := Command{
		OpType: MODIFY_FILE,
		Params: modifiedFileMeta,
	}

	_, err = cp.Process(modifyCommand)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	storedFileMeta, ok := cm.FileMap.Get(originalFileMeta.FileHash)
	if !ok {
		t.Error("FileMetadata not found in FileMetadataStore")
	} else {
		if storedFileMeta.Size != modifiedFileMeta.Size || storedFileMeta.Path[0] != modifiedFileMeta.Path[0] {
			t.Error("Incorrect modification of file metadata")
		}
	}
}

func TestDeleteFileMetaHandler_Execute(t *testing.T) {
	cm := &ConsensusModule{FileMap: NewFileMetadataStore()}
	cp := NewCommandProcessor(cm)

	// 准备测试数据
	fileMeta := &FileMeta{
		FileHash: "test_hash",
		Size:     1024,
		Path:     []string{"path", "to", "file"},
		Ref:      0,
	}
	insertCommand := Command{
		OpType: INSERT_FILE,
		Params: fileMeta,
	}

	_, err := cp.Process(insertCommand)
	if err != nil {
		t.Fatalf("Unexpected error during insert: %v", err)
	}

	// 删除存在的测试数据
	existingDeleteCommand := Command{
		OpType: DELETE_FILE,
		Params: &FileMeta{FileHash: "test_hash"},
	}

	_, err = cp.Process(existingDeleteCommand)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	_, ok := cm.FileMap.Get(fileMeta.FileHash)
	if ok {
		t.Error("FileMetadata still found in FileMetadataStore after deletion")
	}

	// 删除不存在的测试数据
	nonExistingDeleteCommand := Command{
		OpType: DELETE_FILE,
		Params: &FileMeta{FileHash: "non_existing_hash"},
	}

	_, err = cp.Process(nonExistingDeleteCommand)
	if err != nil {
		t.Errorf("Unexpected delete error: %v", err)
	}
}

func TestQueryFileMetaHandler_Execute(t *testing.T) {
	cm := &ConsensusModule{FileMap: NewFileMetadataStore()}
	cp := NewCommandProcessor(cm)

	// 准备测试数据
	fileMeta := &FileMeta{
		FileHash: "test_hash",
		Size:     1024,
		Path:     []string{"path", "to", "file"},
		Ref:      0,
	}

	insertCommand := Command{
		OpType: INSERT_FILE,
		Params: fileMeta,
	}

	_, err := cp.Process(insertCommand)
	if err != nil {
		t.Fatalf("Unexpected error during insert: %v", err)
	}

	// 查询存在的测试数据
	existingQueryCommand := Command{
		OpType: QUERY_FILE,
		Params: &FileMeta{FileHash: "test_hash"},
	}

	queriedData, err := cp.Process(existingQueryCommand)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	queriedFileMeta := queriedData.(*FileMeta)
	if queriedData == nil {
		t.Error("FileMetadata not found in FileMetadataStore")
	} else if queriedFileMeta != fileMeta {
		t.Error("Incorrect query result for existing data")
	}

	// 查询不存在的测试数据
	nonExistingQueryCommand := Command{
		OpType: QUERY_FILE,
		Params: &FileMeta{FileHash: "non_existing_hash"},
	}

	_, err = cp.Process(nonExistingQueryCommand)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestComplexProcess(t *testing.T) {
	cm := &ConsensusModule{FileMap: NewFileMetadataStore()}
	cp := NewCommandProcessor(cm)

	// 准备测试数据
	fileMeta1 := &FileMeta{
		FileHash: "hash1",
		Size:     1024,
		Path:     []string{"path", "to", "file1"},
	}
	fileMeta2 := &FileMeta{
		FileHash: "hash2",
		Size:     2048,
		Path:     []string{"path", "to", "file2"},
	}
	fileMeta3 := &FileMeta{
		FileHash: "hash3",
		Size:     3072,
		Path:     []string{"path", "to", "file3"},
	}

	testCases := []struct {
		TestName string
		command  Command
		run      func(t *testing.T, data interface{})
	}{
		{
			"验证插入第一个文件是否成功",
			Command{
				OpType: INSERT_FILE,
				Params: fileMeta1,
			},
			func(t *testing.T, data interface{}) {
				fileMeta1AfterInsert, ok := cm.FileMap.Get("hash1")
				if !ok {
					t.Error("File not found after insertion")
				} else if fileMeta1AfterInsert.Size != fileMeta1.Size || fileMeta1AfterInsert.Path[0] != fileMeta1.Path[0] {
					t.Error("Incorrect insertion of file metadata")
				}
			},
		},
		{
			"查询第一个文件（存在）",
			Command{
				OpType: QUERY_FILE,
				Params: &FileMeta{FileHash: "hash1"},
			},
			func(t *testing.T, data interface{}) {
				// 验证查询第一个文件是否正确
				fileMeta1AfterInsert := data.(*FileMeta)
				if data == nil {
					t.Error("File not found after insertion")
				} else if fileMeta1AfterInsert.Size != fileMeta1.Size ||
					fileMeta1AfterInsert.Path[0] != fileMeta1.Path[0] || fileMeta1AfterInsert.Ref != 1 {
					t.Error("Incorrect insertion of file metadata")
				}
			},
		},
		{
			"验证第一个文件是否被正确修改",
			Command{
				OpType: MODIFY_FILE,
				Params: &FileMeta{
					FileHash: "hash1",
					Size:     1500,                         // 修改文件大小
					Path:     []string{"updated", "path1"}, // 修改文件路径
					Ref:      0,
				},
			},
			func(t *testing.T, data interface{}) {
				fileMeta1AfterModify, ok := cm.FileMap.Get("hash1")
				if !ok {
					t.Error("File not found after modify")
				} else if fileMeta1AfterModify == nil {
					t.Error("File is nil after modify")
				} else if fileMeta1AfterModify.Path[0] == fileMeta1.Path[0] {
					t.Errorf("File is not modify origFileMeta=%v, modifyFileMeta=%v", fileMeta1, fileMeta1AfterModify)
				}
			},
		},
		{
			"验证删除第一次删除文件是否成功,ref不为0不应该成功",
			Command{
				OpType: DELETE_FILE,
				Params: &FileMeta{FileHash: "hash1"},
			},
			func(t *testing.T, data interface{}) {
				_, ok := cm.FileMap.Get("hash1")
				if !ok {
					t.Error("File should be found after deletion")
				}
			},
		},
		{
			"验证删除第一次删除文件是否成功,ref为0应该成功",
			Command{
				OpType: DELETE_FILE,
				Params: &FileMeta{FileHash: "hash1"},
			},
			func(t *testing.T, data interface{}) {
				_, ok := cm.FileMap.Get("hash1")
				if ok {
					t.Error("File should not be found after deletion")
				}
			},
		},
		{
			"验证查询删除后的第一个文件是否正确",
			Command{
				OpType: QUERY_FILE,
				Params: &FileMeta{FileHash: "hash1"},
			},
			func(t *testing.T, data interface{}) {
				if data != (*FileMeta)(nil) {
					t.Error("Query should not find the deleted file")
				}
			},
		},
		{
			"验证第二个文件是否正确插入",
			Command{
				OpType: INSERT_FILE,
				Params: fileMeta2,
			},
			func(t *testing.T, data interface{}) {
				fileMeta2AfterInsert, ok := cm.FileMap.Get("hash2")
				if !ok {
					t.Error("File not found after insertion")
				} else if fileMeta2AfterInsert.Size != fileMeta2.Size || fileMeta2AfterInsert.Path[0] != fileMeta2.Path[0] {
					t.Error("Incorrect insertion of file metadata")
				}
			},
		},
		{
			"验证第三个文件是否正确插入",
			Command{
				OpType: INSERT_FILE,
				Params: fileMeta3,
			},
			func(t *testing.T, data interface{}) {
				fileMeta3AfterInsert, ok := cm.FileMap.Get("hash3")
				if !ok {
					t.Error("File not found after insertion")
				} else if fileMeta3AfterInsert.Size != fileMeta3.Size || fileMeta3AfterInsert.Path[0] != fileMeta3.Path[0] {
					t.Error("Incorrect insertion of file metadata")
				}
			},
		},
	}

	// 执行测试
	for _, testCase := range testCases {
		data, err := cp.Process(testCase.command)
		if err != nil {
			t.Fatalf("Unexpected error during command %s: %v", testCase.command.OpType, err)
		}
		testCase.run(t, data)
	}
}
