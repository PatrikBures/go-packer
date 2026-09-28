package data

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"go.pabu.dev/packer"
)

const chunkCapacity = 1000

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

func (ms *metadataStore) PutChunkAndAddPending(ds *dataStore, chunk []byte) (packer.ChunkInfo, error) {
	ci, err := ds.Put(chunk)
	if err != nil {
		return ci, err
	}

	ms.mu.Lock()
	ms.pendingPacks[ci.PackID]++
	ms.mu.Unlock()

	return ci, nil
}


func (ms *metadataStore) randChunk(rng *rand.Rand) ([32]byte, packer.ChunkInfo, bool) {
	if ms.count == 0 {
		return [32]byte{}, packer.ChunkInfo{}, false
	}

	item := ms.chunks[rng.IntN(ms.count)]
	return item.hash, item.ci, true
}

func (ms *metadataStore) RandChunk(rng *rand.Rand) ([32]byte, packer.ChunkInfo, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.randChunk(rng)
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
	defer ms.mu.Unlock()
	ms.pendingPacks[packID]++
}

func (ms *metadataStore) RemovePending(packID uint64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.pendingPacks[packID]--
	if ms.pendingPacks[packID] <= 0 {
		delete(ms.pendingPacks, packID)
	}
}

const maxMaxChunkSize = 512<<10 // 512KiB
const minMinChunkSize = 8<<10   // 8KiB
const maxMaxPackSize  = 32<<20  // 32MiB
const minMinPackSize = 1<<20    // 1MiB


func FuzzData1x1(f *testing.F) {
	fuzzData(f, 1, 1)
}

func FuzzData1x3(f *testing.F) {
	fuzzData(f, 1, 3)
}

func FuzzData4x3(f *testing.F) {
	fuzzData(f, 4, 3)
}

func fuzzData(f *testing.F, instances int, routinesPerInstance int) {

	f.Add(uint64(112233))

	f.Fuzz(func(t *testing.T, seed uint64) {
		dir := t.TempDir()

		var seedBytes [32]byte
		binary.LittleEndian.PutUint64(seedBytes[:8], seed)
		cc := rand.NewChaCha8(seedBytes)
		rng := rand.New(cc)

		maxPackSize  := rng.Int64N(maxMaxPackSize - minMinPackSize) + minMinPackSize
		maxChunkSize := rng.Uint32N(maxMaxChunkSize - minMinChunkSize) + minMinChunkSize +1

		stores := make([]*dataStore, 0, instances)
		for range instances {
			ds, err := Init(dir, WithMaxPackSize(maxPackSize))
			if err != nil {
				t.Fatal(err)
			}
			stores = append(stores, ds)
		}

		ms := newMetadataStore()

		var wg sync.WaitGroup
		for i := range instances {
			inst := stores[i]
			for range routinesPerInstance {
				subSeed := rng.Uint64()
				wg.Add(1)

				go func(ds *dataStore, s uint64) {
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

type fuzzAction struct {
	Name   string
	Weight int
	Run    func() error
}

func pickAndRun(actions []fuzzAction, rng *rand.Rand) error {
	totalWeigth := 0
	for _, a := range actions {
		totalWeigth += a.Weight
	}
	if totalWeigth <= 0 {
		return nil
	}

	target := rng.IntN(totalWeigth)

	for _, a := range actions {
		if target < a.Weight {
			if err := a.Run(); err != nil {
				return fmt.Errorf("running action '%s':\n%w", a.Name, err)
			}
			return nil
		}
		target -= a.Weight
	}

	panic("did not run any action")
}

func fuzzDataRandomActions(t *testing.T, ds *dataStore, seed uint64, ms *metadataStore, maxChunkSize uint32) {
	var seedBytes [32]byte
	binary.LittleEndian.PutUint64(seedBytes[:8], seed)
	cc := rand.NewChaCha8(seedBytes)
	rng := rand.New(cc)

	buf := make([]byte, maxChunkSize + prefixLen * 10)

	actions := []fuzzAction{
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
					return fmt.Errorf("getting chunk %x (pack %d):\n%w", hash[:4], ci.PackID, err)
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

				chunkSize := rng.Uint32N(maxChunkSize - minMinChunkSize+1) + minMinChunkSize
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
		{
			Name: "Delete unreferenced packs",
			Weight: 5,
			Run: func() error {
				var nextPackID uint64
				var deletePacks []uint64

				// limit so it does not get stuck
				for range 1000 {
					packs, err := ds.PacksBatch(nextPackID, nil)
					if err != nil {
						return err
					}
					if len(packs) == 0 {
						break
					}

					nextPackID = packs[len(packs)-1] + 1
					packs = ms.FilterExistingPacks(packs)
					deletePacks = append(deletePacks, packs...)
				}

				if _, err := ds.DeletePacks(deletePacks); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("deleting %d packs: %w", len(deletePacks), err)
				}

				return nil
			},
		},
	}

	for i := range 100 {
		if err := pickAndRun(actions, rng); err != nil {
			t.Errorf("action err on iter %d (seed %d): %v", i+1, seed, err)
			return
		}
		ti := time.Duration(rng.Int64N(500)) * time.Microsecond
		time.Sleep(ti)
	}
}

