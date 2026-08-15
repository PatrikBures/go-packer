package packread

import (
	"errors"
	"os"
	"sync"
	"time"
)


type PackRead struct {
	mu sync.Mutex
	packs map[uint64]pack
}

type pack struct {
	lastRead time.Time
	file *os.File
}

func New() PackRead {
	return PackRead{
		packs: make(map[uint64]pack),
	}
}

func (pr *PackRead) Close() error {
	var err error
	for _, p := range pr.packs {
		err = errors.Join(err, p.file.Close())
	}

	return err
}

func (pr *PackRead) Put(packID uint64, f *os.File) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	pr.packs[packID] = pack{
		lastRead: time.Now(),
		file: f,
	}
}

// returns nil if packID does not exist
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
