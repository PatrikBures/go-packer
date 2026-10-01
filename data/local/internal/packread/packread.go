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

// Close specific pack, returns nil if it does not exist.
func (pr *PackRead) ClosePack(packID uint64) error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	p, ok := pr.packs[packID]
	if !ok {
		return nil
	}
	delete(pr.packs, packID)

	return p.file.Close()
}

// Removes pack from cache and returns the removed pack file.
// If provided packID does not exist, os.File is nil.
func (pr *PackRead) GetRemovePack(packID uint64) *os.File {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	p, ok := pr.packs[packID]
	if !ok {
		return nil
	}
	delete(pr.packs, packID)

	return p.file
}

func (pr *PackRead) FilterOlderThan(packIDs []uint64, dur time.Duration) []uint64 {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	var i int
	now := time.Now()

	for _, id := range packIDs {
		p, ok := pr.packs[id]
		if !ok {
			continue
		}

		diff := now.Sub(p.lastRead)
		if diff >= dur {
			packIDs[i] = id
			i++
		}
	}

	return packIDs[:i]
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
