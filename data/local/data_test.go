package local

import (
	"bytes"
	"testing"
)

func TestData(t *testing.T) {
	tempDir := t.TempDir()
	buf := make([]byte, 100)

	ds, err := Init(tempDir, 1)
	if err != nil {
		t.Fatal(err)
	}

	offset := ds.currentPackOffset.Load()
	if offset != headerLen {
		t.Errorf("expected offset to be the len of header. expected: %d, received: %d", headerLen, offset)
	}

	testData := []byte("hello")
	ci, err := ds.Put(testData)
	if err != nil {
		t.Fatalf("putting testdata: %v", err)
	}

	stat, err := ds.currentPackFile.Stat()
	if err != nil {
		t.Fatalf("stating packfile: %v", err)
	}

	expected := headerLen + int64(len(testData)) + 4
	if stat.Size() != expected {
		t.Fatalf("size of pack is not headerLen + testData + 4. expected: %d, received: %d", expected, stat.Size())
	}

	testDataOut, err := ds.Get(ci, buf)
	if err != nil {
		t.Fatalf("getting testdata: %v", err)
	}

	if !bytes.Equal(testData, testDataOut) {
		t.Fatalf("expected: %s, received: %s", string(testData), string(testDataOut))
	}
}
