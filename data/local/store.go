package data

import (
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"go.pabu.dev/packer/data/local/internal/packread"
	"go.pabu.dev/packer/data/local/internal/pulser"
)

const minPackSize = 1024 << 10 // 1MiB
const prefixLen = 4

type dataStore struct {
	packDir string

	currentPackFile   *os.File
	currentPackOffset int64
	currentPackID     uint64

	writePackMu sync.Mutex

	packPulse *pulser.Pulse

	// options
	compactPackRatio  float64
	maxPackSize       int64
	growBuf           bool

	// how old the ModTime needs to be for a lock to consider it stale and available.
	lockStaleDuration time.Duration 

	// how often the lock ModTime should update.
	lockPulseInterval time.Duration

	packRead packread.PackRead
}


type Option func(ds *dataStore)

// If bytes is less than 1MiB, it will clamp to 1MiB.
// Default is 64MiB.
func WithMaxPackSize(bytes int64) Option {
	return func(ds *dataStore) {
		ds.maxPackSize = max(minPackSize, bytes)
	}
}

// Ratio of existing chunks in a pack compared with the total size of the pack.
// Ratio is clamped between 0 and 1.
// Default is 0.25
func WithCompactPackThreshold(ratio float64) Option {
	return func(ds *dataStore) {
		ds.compactPackRatio = min(max(ratio, 0), 1)
	}
}

// Instead of returning and error when getting a chunk
// if the chunk won't fit in the buf, grow the buf.
// It should mainly be used if the size limit of chunks is not known.
// Default is false
func WithGrowBuf(b bool) Option {
	return func(ds *dataStore) {
		ds.growBuf = b
	}
}

// The duration it takes for a pack to be considered stale and
// available to be continued.
// Clamped to min 10sec.
// Default is 10min
func WithLockStaleDuration(dur time.Duration) Option {
	return func(ds *dataStore) {
		ds.lockStaleDuration = min(dur, time.Second*10)
	}
}


// The interval on how often it updates the ModTime for a pack
// so it does not become considered stale.
// Clamped to min 1sec.
// Default is 30sec
func WithLockPulseInterval(dur time.Duration) Option {
	return func(ds *dataStore) {
		ds.lockPulseInterval = min(dur, time.Second)
	}
}

// Returns the dataStore object.
// Panics if LockPulseInterval is NOT twice as big as LockStaleDuration.
func New(packDir string, opts ...Option) *dataStore {

	ds := dataStore{
		packDir:           packDir,
		packRead:          packread.New(),
		maxPackSize:       1024 << 16, // 64MiB
		growBuf:           false,
		lockStaleDuration: time.Minute*10,
		lockPulseInterval: time.Second*30,
		compactPackRatio:  0.25,
	}

	for _, opt := range opts {
		opt(&ds)
	}

	if ds.lockPulseInterval*2 > ds.lockStaleDuration {
		panic(fmt.Errorf("LockPulseInterval needs to be at least twice as big as LockStaleDuration"))
	}

	if math.IsInf(ds.compactPackRatio, 0) || math.IsNaN(ds.compactPackRatio) {
		panic(fmt.Errorf("CompactPackRatio is either Inf or NaN"))
	}

	return &ds
}

func (ds *dataStore) Init() error {
	for _, p := range []string{
		ds.completedDir(),
		ds.activeDir(),
	} {
		if err := os.MkdirAll(p, 0755); err != nil {
			return fmt.Errorf("creating dir: %w", err)
		}
	}

	if err := ds.SwitchPackExisting(0); err != nil {
		return err
	}

	return nil
}

// Returns error for closing write pack
func (ds *dataStore) Close() error {
	if ds.packPulse != nil {
		ds.packPulse.Close()
	}
	ds.packRead.Close()
	return ds.currentPackFile.Close()
}

func (ds *dataStore) CurrentPackID() uint64 {
	return ds.currentPackID
}

func (ds *dataStore) MaxPackSize() int64 {
	return ds.maxPackSize
}
