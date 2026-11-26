package packwrite

import (
	"fmt"
	"sync"
)

type PackWrite struct {
	mu sync.Mutex
	packs map[uint64]int // id and number of writers
}

// Creates new PackWrite object.
// used for tracking the number of routines
// writing to that same pack to ensure it is not
// closed when a different routine is writing to it.
// 
// All methods which modify are concurrently safe.
func New() PackWrite {
	return PackWrite{
		packs: make(map[uint64]int),
	}
}

// Adds increments the number of writer to pack.
func (pw *PackWrite) Add(packID uint64) {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	pw.packs[packID]++
}

// Decrements the amount of writers for pack, if it becomes zero
// it returns true, meaning the file can be closed.
//
// If the provided pack does not exist, or decrementing
// would make the number of zeros be below 0, it panics.
func (pw *PackWrite) Dec(packID uint64) bool {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	n, exists := pw.packs[packID]
	if !exists {
		panic(fmt.Errorf("tried decrementing non-existing pack"))
	}

	if n == 1 {
		delete(pw.packs, packID)
		return true
	} else if n > 1 {
		pw.packs[packID]--
	} else {
		panic(fmt.Errorf("count for writers is 0 or less and can't decrement more"))
	}
	
	return false
}


// Returns true if a routine is currently writing to the pack.
func (pw *PackWrite) Exists(packID uint64) bool {
	_, exists := pw.packs[packID]
	return exists
}
