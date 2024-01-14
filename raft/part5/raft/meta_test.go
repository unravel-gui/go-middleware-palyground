package raft

import (
	"testing"
)

func TestFileMetadataStoreUpsert(t *testing.T) {
	store := NewFileMetadataStore()

	fileMeta := &FileMeta{
		FileHash: "hash1",
		Size:     1024,
		Path:     []string{"dir", "file.txt"},
	}

	store.Upsert(fileMeta)

	// Check if the data was inserted correctly
	result, ok := store.Get("hash1")
	if !ok || result.Ref != 1 {
		t.Errorf("Upsert failed. Expected: %v, Got: %v", fileMeta, result)
	}
	store.Upsert(fileMeta)
	result, ok = store.Get("hash1")
	if !ok || result.Ref != 2 {
		t.Errorf("Upsert failed. Expected: %v, Got: %v", fileMeta, result)
	}
}

func TestFileMetadataStoreGet(t *testing.T) {
	store := NewFileMetadataStore()

	fileMeta := &FileMeta{
		FileHash: "hash2",
		Size:     2048,
		Path:     []string{"dir", "file2.txt"},
	}

	store.Upsert(fileMeta)

	result, ok := store.Get("hash2")
	if !ok || result.Ref != 1 {
		t.Errorf("Get failed. Expected: %v, Got: %v", fileMeta, result)
	}

	result, ok = store.Get("hash0")
	if ok {
		t.Errorf("Remove failed. Data should not exists for key 'hash0'")
	}
}

func TestFileMetadataStoreRemove(t *testing.T) {
	store := NewFileMetadataStore()

	fileMeta := &FileMeta{
		FileHash: "hash3",
		Size:     3072,
		Path:     []string{"dir", "file3.txt"},
	}

	store.Upsert(fileMeta)
	store.Upsert(fileMeta)
	store.Remove("hash3")

	_, ok := store.Get("hash3")
	if ok {
		t.Errorf("Remove failed. Data still exists for key 'hash3'")
	}
}

func TestFileMetadataStoreDelete(t *testing.T) {
	store := NewFileMetadataStore()

	fileMeta := &FileMeta{
		FileHash: "hash4",
		Size:     4096,
		Path:     []string{"dir", "file4.txt"},
		Ref:      2,
	}

	store.Upsert(fileMeta)

	store.Delete("hash4")

	_, ok := store.Get("hash4")
	if ok {
		t.Errorf("Delete failed. Data still exists for key 'hash4'")
	}

	fileMeta.Ref = 1
	store.Upsert(fileMeta)
	store.Upsert(fileMeta)
	store.Delete("hash4")
	result, ok := store.Get("hash4")
	if !ok || result.Ref != 1 {
		t.Errorf("Delete failed. Expected: %v, Got: %v", fileMeta, result)
	}
}

func TestFileMetadataStoreLength(t *testing.T) {
	store := NewFileMetadataStore()

	fileMeta1 := &FileMeta{
		FileHash: "hash5",
		Size:     5120,
		Path:     []string{"dir", "file5.txt"},
	}

	fileMeta2 := &FileMeta{
		FileHash: "hash6",
		Size:     6144,
		Path:     []string{"dir", "file6.txt"},
	}

	store.Upsert(fileMeta1)

	length := store.Length()
	if length != 1 {
		t.Errorf("Length after Upsert failed. Expected: 1, Got: %d", length)
	}

	store.Upsert(fileMeta2)

	length = store.Length()
	if length != 2 {
		t.Errorf("Length after additional Upsert failed. Expected: 2, Got: %d", length)
	}

	store.Remove("hash5")

	length = store.Length()
	if length != 1 {
		t.Errorf("Length after Remove failed. Expected: 1, Got: %d", length)
	}
}
