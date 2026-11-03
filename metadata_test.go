package packer

import "testing"

func TestChunkInfoBytes(t *testing.T) {
	ci := ChunkInfo{
		PackID: 89823748273,
		Offset: 892787,
		Size:   1111111111,
	}
	buf := make([]byte, ChunkInfoSize)
	ci.PutBytes(buf)

	receivedCi, err := BytesToChunkInfo(buf)
	if err != nil {
		t.Fatal(err)
	}

	if receivedCi != ci {
		t.Fatalf("%+v != %+v", receivedCi, ci)
	}
}

func TestPackInfoBytes(t *testing.T) {
	pi := PackInfo{
		Size:       1928734,
		ChunkCount: 33892837,
		State:      PackStateCompleted,
	}
	buf := make([]byte, PackInfoSize)
	pi.PutBytes(buf)

	receivedPi, err := BytesToPackInfo(buf)
	if err != nil {
		t.Fatal(err)
	}
	if receivedPi != pi {
		t.Fatalf("%+v != %+v", receivedPi, pi)
	}
}
