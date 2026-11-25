package datatest

import (
	"io"
	"slices"
	"testing"

	"go.pabu.dev/packer"
)

type StoreFactory func(t *testing.T, dir string, maxPackSize int64) packer.DataStore

func RunSuite(t *testing.T, factory StoreFactory) {
	tests := []struct {
		name string
		run  func(t *testing.T, ds packer.DataStore)
	}{
		{
			name: "GetReader",
			run:  testGetReader,
		},
		{
			name: "Get",
			run:  testGet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			ds := factory(t, dir, 1<<20)
			tt.run(t, ds)
		})
	}
}


func testGetReader(t *testing.T, ds packer.DataStore) {
	in := []byte("hello world!!!")
	ci, err := ds.Put(in)
	if err != nil {
		t.Fatal(err)
	}

	r, err := ds.GetReader(ci)
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(in, out) {
		t.Fatalf("out does not equal input\nin: %s\nout: %s", in, out)
	}
}

func testGet(t *testing.T, ds packer.DataStore) {
	in := []byte("hello world 12345")
	ci, err := ds.Put(in)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 100)

	gout, err := ds.Get(ci, buf)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(in, gout) {
		t.Fatalf("out does not equal input\nin: %s\nout: %s", in, gout)
	}
}
