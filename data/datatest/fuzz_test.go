package datatest

import (
	"slices"
	"testing"
)

func TestFilterExistingPacks(t *testing.T) {
	ms := metadataStore{}
	ms.packs = map[uint64]int{
		3: 1,
		5: 1,
		8: 1,
	}

	ms.pendingPacks = map[uint64]int{
		2: 1,
	}

	input := []uint64{0,1,2,3,4,5,6,7,8,9}
	want := []uint64{0,1,4,6,7,9}

	output := ms.FilterExistingPacks(input)

	if !slices.Equal(want, output) {
		t.Fatalf("FilterExistingPacks(%v) = %v, want %v", input, output, want)
	}
}
