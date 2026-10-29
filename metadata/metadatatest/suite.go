package metadatatest

import (
	"slices"
	"testing"

	"go.pabu.dev/packer"
)

type StoreFactory func(t *testing.T) packer.MetadataStore

func RunSuite(t *testing.T, factory StoreFactory) {
	tests := []struct{
		name string
		run func(t *testing.T, ms packer.MetadataStore)
	} {
		{
			name: "PutAndGetSingleChunk",
			run: testPutAndGetSingleChunk,
		},
		{
			name: "PutChunksBatch",
			run: testPutChunkBatch,
		},
		{
			name: "GetAllIDs",
			run: testGetAllIDs,
		},
		{
			name: "Delete",
			run: testDelete,
		},
		{
			name: "UpdateChunk",
			run: testUpdateChunk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := factory(t)
			tt.run(t, ms)
		})
	}
}

func testPutAndGetSingleChunk(t *testing.T, ms packer.MetadataStore) {
	testID := []byte("hello")
	testCI := packer.ChunkInfo{
		PackID: 1,
		Offset: 1234,
		Size: 10,
	}

	if err := ms.Put(testID, testCI); err != nil {
		t.Fatal(err)
	}

	if exists, err := ms.Exists(testID); err != nil {
		t.Fatal(err)
	} else if !exists {
		t.Errorf("expected id '%s' to exist", testID)
	}

	receivedCI, err := ms.Get(testID)
	if err != nil {
		t.Fatal(err)
	}

	if testCI != receivedCI {
		t.Fatalf("input ci does not match received ci:\nin:  %+v\nout: %+v", testCI, receivedCI)
	}
}
func testPutChunkBatch(t *testing.T, ms packer.MetadataStore) {
	if err := ms.PutChunks(testDataChunks); err != nil {
		t.Fatal(err)
	}

	receivedIDs, err := ms.ChunkIDsAll()
	if err != nil {
		t.Fatal(err)
	}

	verifyReceivedChunks(t, ms, receivedIDs)
}

func testGetAllIDs(t *testing.T, ms packer.MetadataStore) {

	for _, td := range testDataChunks {
		if err := ms.Put(td.Id, td.ChunkInfo); err != nil {
			t.Error(err)
		}
	}

	allIds, err := ms.ChunkIDsAll()
	if err != nil {
		t.Fatal(err)
	}

	verifyReceivedChunks(t, ms, allIds)
}

func testDelete(t *testing.T, ms packer.MetadataStore) {
	if err := ms.PutChunks(testDataChunks); err != nil {
		t.Fatal(err)
	}

	deleteIds := [][]byte{
		testDataChunks[1].Id,
		testDataChunks[2].Id,
	}

	chunksExist(t, ms, deleteIds, true, "before deletion")

	if err := ms.Delete(deleteIds); err != nil {
		t.Fatal(err)
	}

	chunksExist(t, ms, deleteIds, false, "after deletion")
}

func testUpdateChunk(t *testing.T, ms packer.MetadataStore) {

	chunk := packer.ChunkInfoWithID{
		Id: []byte("hello there"),
		ChunkInfo: packer.ChunkInfo{
			PackID: 1,
			Offset: 2,
			Size: 1,
		},
	}

	if err := ms.Put(chunk.Id, chunk.ChunkInfo); err != nil {
		t.Fatal(err)
	}

	newCI, err := ms.Get(chunk.Id)
	if err != nil {
		t.Fatal(err)
	}

	if newCI != chunk.ChunkInfo {
		t.Fatalf("want ci: %+v\nreceived: %+v", chunk.ChunkInfo, newCI)
	}


	pi, err := ms.PackInfo([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}

	if pi[1].ChunkCount != 1 {
		t.Fatal("expected chunk count to be 1 for pack 1, received: %d", pi[1].ChunkCount)
	}


	// marks pack as completed so it can be marked for deletion, as only completed packs can do that.
	if err := ms.MarkCompleted(chunk.PackID); err != nil {
		t.Fatal(err)
	}

	chunk.PackID = 2

	if err := ms.Put(chunk.Id, chunk.ChunkInfo); err != nil {
		t.Fatal(err)
	}

	newCI, err = ms.Get(chunk.Id)

	if newCI != chunk.ChunkInfo {
		t.Fatalf("want ci: %+v\nreceived: %+v", chunk.ChunkInfo, newCI)
	}

	pi, err = ms.PackInfo([]uint64{1, 2})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := pi[1]; ok {
		t.Fatal("expected pack 1 to not exist")
	}

	if pi[2].ChunkCount != 1 {
		t.Fatal("expected chunk count to be 1 for pack 2, received: %d", pi[2].ChunkCount)
	}
}

func chunksExist(t *testing.T, ms packer.MetadataStore, ids [][]byte, shouldExist bool, msg string) {
	for _, id := range ids {
		if exists, err := ms.Exists(id); err != nil {
			t.Fatal(err)
		} else if exists != shouldExist {
			s := "exist"
			if !shouldExist {
				s = "NOT exist"
			}
			t.Errorf("expected chunk with id '%s' to %s %s", id, s, msg)
		}

	}
}


func verifyReceivedChunks(t *testing.T, ms packer.MetadataStore, ids [][]byte) {
	for _, td := range testDataChunks {
		exists := false
		for _, id := range ids {
			if slices.Equal(td.Id, id) {
				exists = true
				break
			}
		}
		if !exists {
			t.Errorf("id '%s' not found in metadataStore after using PutChunks", td.Id)
		}

		receivedCI, err := ms.Get(td.Id)
		if err != nil {
			t.Fatal(err)
		}

		if td.ChunkInfo != receivedCI {
			t.Errorf("testdata CI for id '%s' does not match received CI: \nwant: %+v\nreceived: %+v", td.Id, td.ChunkInfo, receivedCI)
		}
	}
}



var testDataChunks = []packer.ChunkInfoWithID{
	{
		Id: []byte("hello there"),
		ChunkInfo: packer.ChunkInfo{
			PackID: 1,
			Offset: 2,
			Size: 1,
		},
	},
	{
		Id: []byte("hello there asdf"),
		ChunkInfo: packer.ChunkInfo{
			PackID: 1,
			Offset: 138782,
			Size: 10000,
		},
	},
	{
		Id: []byte("asdfasdf"),
		ChunkInfo: packer.ChunkInfo{
			PackID: 3,
			Offset: 1000,
			Size: 12341234,
		},
	},
}

