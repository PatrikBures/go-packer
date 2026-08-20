package packread

import (
	"os"
	"sync"
	"time"
)

type PackRead struct {
	mu    sync.Mutex
	packs map[uint64]pack
}

type pack struct {
	lastRead time.Time
	file     *os.File
}

func New() PackRead {
	return PackRead{
		packs: make(map[uint64]pack),
	}
}

// Puts read pack into cache
func (pr *PackRead) Put(packID uint64, f *os.File) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	pr.packs[packID] = pack{
		lastRead: time.Now(),
		file:     f,
	}
}

// Gets pack from cache and updates lastRead.
// Returns nil if packID does not exist.
func (pr *PackRead) Get(packID uint64) *os.File {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	p, ok := pr.packs[packID]
	if !ok {
		return nil
	}

	p.lastRead = time.Now()

	pr.packs[packID] = p

	return p.file
}

// Closes packs which were last accessed more than dur and removing them from the cache.
//
// This is intended to be ran continuously.
func (pr *PackRead) CloseOlderThan(dur time.Duration) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	now := time.Now()

	for k, v := range pr.packs {
		diff := now.Sub(v.lastRead)
		if diff >= dur {
			_ = v.file.Close() // errors do not really matter as we only read from the file
			delete(pr.packs, k)
		}
	}
}

// Removes and closes every cached file.
func (pr *PackRead) Close() {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	for k, v := range pr.packs {
		_ = v.file.Close()
		delete(pr.packs, k)
	}
}
