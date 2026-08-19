package packer

import "testing"

func TestChunkInfoBytes(t *testing.T) {
	ci := ChunkInfo{
		PackID:    89823748273,
		Offset:    892787,
		SizeBytes: 1111111111,
	}
	buf := make([]byte, 20)
	ci.PutBytes(buf)

	receivedCi, err := BytesToChunkInfo(buf)
	if err != nil {
		t.Fatal(err)
	}

	if receivedCi != ci {
		t.Fatalf("%+v != %+v", receivedCi, ci)
	}
}
