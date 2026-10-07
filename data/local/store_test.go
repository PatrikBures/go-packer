package data

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"go.pabu.dev/packer"
)

func TestData(t *testing.T) {
	tempDir := t.TempDir()
	buf := make([]byte, 100)

	ds, err := Init(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	if ds.currentPackOffset != headerLen {
		t.Errorf("expected offset to be the len of header. expected: %d, received: %d", headerLen, ds.currentPackOffset)
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




func TestPutChunk(t *testing.T) {
	dir := t.TempDir()

	filePath := filepath.Join(dir, "pack.bin")

	f, err := os.OpenFile(filePath, os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0666)
	if err != nil {
		t.Fatal(err)
	}

	offset := int64(headerLen)

	testData := []struct{
		chunk []byte
		ci packer.ChunkInfo
	} {
		{ chunk: []byte("hello there") },
		{ chunk: []byte("hello there asdf") },
		{ chunk: []byte("asdfasdf") },
		{ chunk: []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") },
		{ chunk: []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") },
		{ chunk: []byte("c") },
		{ chunk: []byte("") },
	}

	firstTd := testData[0]

	firstTd.ci, err = putChunk(1, firstTd.chunk, f, offset)
	if err != nil {
		t.Fatal(err)
	}

	if firstTd.ci.Offset != 0 {
		t.Fatal("expected offset to be 0 on first chunk, was: %w", firstTd.ci.Offset)
	}

	testData[0] = firstTd

	for i, td := range testData[1:] {
		td.ci, err = putChunk(1, td.chunk, f, offset)
		if err != nil {
			t.Fatal(err)
		}
		testData[i] = td

		if int(td.ci.SizeBytes) != len(td.chunk) {
			t.Errorf("ci.SizeBytes = %d, want %d", td.ci.SizeBytes, len(td.chunk))
		}

		offset += int64(td.ci.SizeBytes) + prefixLen
	}
}
