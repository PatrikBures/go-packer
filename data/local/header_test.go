package local

import (
	"math"
	"slices"
	"testing"
)

func TestHeaderBytes(t *testing.T) {
	h := header{
		magic:   []byte("PACK"),
		version: 3,
		packID:  89,
	}

	b := h.Bytes()

	expect := []byte{80, 65, 67, 75, 3, 0, 0, 0, 89, 0, 0, 0, 0, 0, 0, 0}
	if !slices.Equal(expect, b) {
		t.Fatalf("bytes not equal\nexpected: %v\nreceived: %v", expect, b)
	}
}

func TestHeader(t *testing.T) {
	testData := []header{
		{
			magic:   []byte("asdf"),
			version: math.MaxInt32,
			packID:  math.MaxUint64,
		},
		{
			magic:   []byte("TEST"),
			version: 0,
			packID:  0,
		},
		{
			magic:   []byte("AA3a"),
			version: 87234,
			packID:  898273480971,
		},
	}

	for _, td := range testData {
		t.Run(string(td.magic), func(t *testing.T) {
			b := td.Bytes()
			n, err := toHeader(b)
			if err != nil {
				t.Error(err)
			}

			if !td.Equal(n) {
				t.Errorf("not equal\nexpected: %+v\nrecevived: %+v", td, n)
			}
		})
	}
}
