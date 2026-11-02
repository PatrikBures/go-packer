package data

import (
	"errors"
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
	ds := New(dir, WithMaxPackSize(maxPackSize))
	if err := ds.Init(); errors.Is(err, packer.ErrNewPack) {
		if err := ds.SwitchPack(1); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	return ds
}
