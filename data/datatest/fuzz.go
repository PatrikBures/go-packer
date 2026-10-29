package datatest

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"testing"

	"go.pabu.dev/packer"
	"go.pabu.dev/packer/packertest"
)

const chunkCapacity = 250

type trackedChunk struct {
	hash [32]byte
	ci   packer.ChunkInfo
}

type metadataStore struct {
	mu            sync.Mutex

	chunks        [chunkCapacity]trackedChunk
	head          int
	count         int

	packs         map[uint64]int // stores chunk ref count
	pendingPacks  map[uint64]int // in-flight read/writes
}

func newMetadataStore() *metadataStore {
	return &metadataStore{
		packs:        make(map[uint64]int),
		pendingPacks: make(map[uint64]int),
	}
}

func (ms *metadataStore) Put(hash [32]byte, ci packer.ChunkInfo) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.count == chunkCapacity {
		oldest := ms.chunks[ms.head]

		ms.packs[oldest.ci.PackID]--
		if ms.packs[oldest.ci.PackID] <= 0 {
			delete(ms.packs, oldest.ci.PackID)
		}
	} else {
		ms.count ++
	}

	ms.chunks[ms.head] = trackedChunk{hash: hash, ci: ci}
	ms.packs[ci.PackID]++
	ms.head = (ms.head+1) % chunkCapacity
}

func (ms *metadataStore) PutChunkAndAddPending(ds packer.DataStore, chunk []byte) (packer.ChunkInfo, error) {
	ci, err := ds.Put(chunk)
	if err != nil {
		return ci, err
	}

	ms.AddPending(ci.PackID)

	return ci, nil
}


// get random chunk with its hash, ChunkInfo and a bool which is only false if there are no chunks.
func (ms *metadataStore) randChunk(rng *rand.Rand) ([32]byte, packer.ChunkInfo, bool) {
	if ms.count == 0 {
		return [32]byte{}, packer.ChunkInfo{}, false
	}

	item := ms.chunks[rng.IntN(ms.count)]
	return item.hash, item.ci, true
}

func (ms *metadataStore) RandChunkAndAddPending(rng *rand.Rand) ([32]byte, packer.ChunkInfo, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	hash, ci, ok := ms.randChunk(rng)
	if !ok {
		return hash, ci, false
	}
	ms.pendingPacks[ci.PackID]++

	return hash, ci, ok
}

func (ms *metadataStore) FilterExistingPacks(packs []uint64) []uint64 {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var n int
	for _, packID := range packs {
		_, inUse := ms.packs[packID]
		_, inFlight := ms.pendingPacks[packID]

		// only filters out if unreferenced and not currently in-flight
		if !inUse && !inFlight {
			packs[n] = packID
			n++
		}
	}
	return packs[:n]
}

func (ms *metadataStore) AddPending(packID uint64) {
	ms.mu.Lock()
	ms.pendingPacks[packID]++
	ms.mu.Unlock()
}

func (ms *metadataStore) RemovePending(packID uint64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.pendingPacks[packID]--
	if ms.pendingPacks[packID] <= 0 {
		delete(ms.pendingPacks, packID)
	}
}


type StoreFactory func(t *testing.T, dir string, maxPackSize int64) packer.DataStore


func FuzzData(f *testing.F, factory StoreFactory, instances int, routinesPerInstance int) {

	f.Add(uint64(112233))

	f.Fuzz(func(t *testing.T, seed uint64) {
		dir := t.TempDir()

		var seedBytes [32]byte
		binary.LittleEndian.PutUint64(seedBytes[:8], seed)
		cc := rand.NewChaCha8(seedBytes)
		rng := rand.New(cc)

		maxPackSize  := rng.Int64N(packertest.MaxMaxPackSize - packertest.MinMinPackSize) + packertest.MinMinPackSize
		maxChunkSize := rng.Uint32N(packertest.MaxMaxChunkSize - packertest.MinMinChunkSize) + packertest.MinMinChunkSize +1

		stores := make([]packer.DataStore, 0, instances)
		for range instances {
			ds := factory(t, dir, maxPackSize)
			stores = append(stores, ds)
		}

		ms := newMetadataStore()

		var wg sync.WaitGroup
		for i := range instances {
			inst := stores[i]
			for range routinesPerInstance {
				subSeed := rng.Uint64()
				wg.Add(1)

				go func(ds packer.DataStore, s uint64) {
					defer wg.Done()

					defer func() {
						if r := recover(); r != nil {
							t.Errorf("goroutine panicked: %v\n%s", r, debug.Stack())
						}
					}()

					fuzzDataRandomActions(t, ds, s, ms, maxChunkSize)
				}(inst, subSeed)
			}
		}

		wg.Wait()
	})
}


func fuzzDataRandomActions(t *testing.T, ds packer.DataStore, seed uint64, ms *metadataStore, maxChunkSize uint32) {
	var seedBytes [32]byte
	binary.LittleEndian.PutUint64(seedBytes[:8], seed)
	cc := rand.NewChaCha8(seedBytes)
	rng := rand.New(cc)

	buf := make([]byte, maxChunkSize + 1024)

	actions := []packertest.FuzzAction{
		{
			Name: "Read random chunk",
			Weight: 500,
			Run: func() error {
				hash, ci, ok := ms.RandChunkAndAddPending(rng)
				if !ok {
					return nil
				}
				defer ms.RemovePending(ci.PackID)

				data, err := ds.Get(ci, buf)
				if err != nil {
					return fmt.Errorf("getting chunk %x (pack %d): %w", hash[:4], ci.PackID, err)
				}

				if hashData := sha256.Sum256(data); hashData != hash {
					return fmt.Errorf("hash mismatch: expected %x, got %x", hash[:4], hashData[:4])
				}

				return nil
			},
		},
		{
			Name: "Write random chunk",
			Weight: 50,
			Run: func() error {

				chunkSize := rng.Uint32N(maxChunkSize - packertest.MinMinChunkSize+1) + packertest.MinMinChunkSize
				chunkBytes := buf[:chunkSize]

				if _, err := cc.Read(chunkBytes); err != nil {
					return fmt.Errorf("reading random bytes into buffer: %w", err)
				}

				
				ci, err := ms.PutChunkAndAddPending(ds, chunkBytes)
				if err != nil {
					return fmt.Errorf("putting chunk: %w", err)
				}
				defer ms.RemovePending(ci.PackID)

				hash := sha256.Sum256(chunkBytes)

				ms.Put(hash, ci)

				chunkBytesGet, err := ds.Get(ci, buf)
				if err != nil {
					return fmt.Errorf("getting chunk after putting: %w", err)
				}

				if hashGet := sha256.Sum256(chunkBytesGet); hash != hashGet {
					return fmt.Errorf("hash mismatch after put: expected %x, got: '%x'", hash[:4], hashGet[:4])
				}

				return nil
			},
		},
	}

	for i := range 50 {
		if err := packertest.PickAndRunAction(actions, rng); err != nil {
			t.Errorf("action err on iter %d (seed %d): %v", i+1, seed, err)
			return
		}
	}
}

