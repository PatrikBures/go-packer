package data

import (
	"fmt"
	"os"
	"sync"

	"github.com/gofrs/flock"
	"go.pabu.dev/packer/data/local/internal/packread"
)

type dataStore struct {
	packDir string

	currentPackFile   *os.File
	currentPackLock   *flock.Flock
	currentPackOffset int64
	currentPackID     uint64

	writePackMu       sync.Mutex

	// options
	compactPackRatio  float64
	maxPackSize       int64
	growBuf           bool

	packRead          packread.PackRead
}

const minPackSize = 1024 << 10     // 1MiB
const defaultPackSize = 1024 << 16 // 64MiB

const prefixLen = 4

type Option func(ds *dataStore)

// If bytes is less than 1MiB, it will clamp to 1MiB.
// Default is 64MiB.
func WithMaxPackSize(bytes int64) Option {
	return func(ds *dataStore) {
		ds.maxPackSize = max(minPackSize, bytes)
	}
}

// Ratio of existing chunks in a pack compared with the total size of the pack.
//
// Ratio is clamped between 0 and 1.
//
// Default is 0.25
func WithCompactPackThreshold(ratio float64) Option {
	return func(ds *dataStore) {
		ds.compactPackRatio = max(min(ratio, 1), 0)
	}
}

// instead of returning and error when getting a chunk
// if the chunk won't fit in the buf, grow the buf.
// it should mainly be used if the size limit of chunks is not known.
func WithGrowBuf(b bool) Option {
	return func(ds *dataStore) {
		ds.growBuf = b
	}
}

// Returns the dataStore object
func New(packDir string, opts ...Option) *dataStore {

	ds := dataStore{
		packDir:          packDir,
		packRead:         packread.New(),
		maxPackSize:      defaultPackSize,
		compactPackRatio: 0.25,
	}

	for _, opt := range opts {
		opt(&ds)
	}

	return &ds
}

func (ds *dataStore) Init() error {
	for _, p := range []string{
		ds.completedDir(),
		ds.activeDir(),
	} {
		if err := os.MkdirAll(p, 0755); err != nil{
			return fmt.Errorf("creating dir: %w", err)
		}
	}

	if err := ds.SwitchPackExisting(); err != nil {
		return err
	}

	return nil
}

// Returns error for closing write pack
func (ds *dataStore) Close() error {
	ds.packRead.Close()
	return ds.currentPackFile.Close()
}


func (ds *dataStore) CurrentPackID() uint64 {
	return ds.currentPackID
}

