package data

import (
	"testing"

	"go.pabu.dev/packer"
	"go.pabu.dev/packer/data/datatest"
)

func FuzzData1x1(f *testing.F) {
	datatest.FuzzData(f, initData, 1, 1)
}

func FuzzData1x3(f *testing.F) {
	datatest.FuzzData(f, initData, 1, 3)
}

func FuzzData4x3(f *testing.F) {
	datatest.FuzzData(f, initData, 4, 3)
}

func FuzzData12x2(f *testing.F) {
	datatest.FuzzData(f, initData, 12, 2)
}

func initData(t *testing.T, dir string, maxPackSize int64) packer.DataStore {
	ds, err := Init(dir, WithMaxPackSize(maxPackSize))
	if err != nil {
		t.Fatal(err)
	}
	return ds
}
