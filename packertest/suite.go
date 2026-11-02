package packertest

import (
	"crypto/sha256"
	"slices"
	"testing"

	"go.pabu.dev/packer"
)

type StoreFactory func(t *testing.T) *packer.Packager

func RunSuite(t *testing.T, factory StoreFactory) {
	tests := []struct{
		name string
		run func(t *testing.T, p *packer.Packager)
	} {
		{
			name: "PutGet",
			run: testPutGet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := factory(t)
			tt.run(t, p)
		})
	}
}


func testPutGet(t *testing.T, p *packer.Packager) {

	data := []byte("hello there asdf")
	hash := sha256.Sum256(data)

	err := p.Put(hash[:], data)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 128)

	dataReceived, err := p.Get(hash[:], buf)

	if !slices.Equal(data, dataReceived) {
		t.Fatal("did not receive same data\nwant: %s\nreceived: %s", data, dataReceived)
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

