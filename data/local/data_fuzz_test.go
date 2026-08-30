package data

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"go.pabu.dev/packer"
)


type metadataStore struct {
	Chunks       map[[32]byte]packer.ChunkInfo
	mu           sync.Mutex
	maxChunkSize uint32
	packs        map[uint64]struct{}
}

func randItem[K comparable, V any](m map[K]V, rng *rand.Rand) (k K, v V, ok bool) {
	if len(m) == 0 {
		return k, v, false
	}

	target := rand.IntN(len(m))

	i := 0
	for k, v := range m {
		if i == target {
			return k, v, true
		}
		i++
	}

	return k, v, false
}


const maxMaxChunkSize = 512<<10 // 512KiB
const minMinChunkSize = 8<<10   // 8KiB
const maxMaxPackSize  = 32<<20  // 32MiB
const minMinPackSize = 1<<20    // 1MiB

func FuzzData(f *testing.F) {

	f.Add(uint64(112233))

	f.Fuzz(func(t *testing.T, seed uint64) {
		dir := t.TempDir()
		instances := 4


		var seedBytes [32]byte
		binary.LittleEndian.PutUint64(seedBytes[:8], seed)
		cc := rand.NewChaCha8(seedBytes)
		rng := rand.New(cc)

		maxPackSize  := rng.Int64N(maxMaxPackSize - minMinPackSize) + minMinPackSize
		maxChunkSize := rng.Uint32N(maxMaxChunkSize - minMinChunkSize) + minMinChunkSize

		stores := make([]*dataStore, 0, instances)
		for range instances {
			s, err := Init(dir, WithMaxPackSize(maxPackSize))
			if err != nil {
				t.Fatal(err)
			}
			stores = append(stores, s)
		}

		ms := &metadataStore{
			Chunks: make(map[[32]byte]packer.ChunkInfo),
			packs: make(map[uint64]struct{}),
			maxChunkSize: maxChunkSize,
		}

		i := 0
		for r := range instances * 3 {
			if i == 4 {
				i = 0
			}

			inst := stores[i]

			t.Run(fmt.Sprintf("instance: %d, routine: %d", i, r), func(t *testing.T) {
				t.Parallel()
				fuzzDataRandomActions(t, inst, rng.Uint64(), ms)
			})

			i++
		}
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
				return fmt.Errorf("running action '%s': %w", a.Name, err)
			}
			return nil
		}
		target -= a.Weight
	}

	panic("did not run any action")
}

func fuzzDataRandomActions(t *testing.T, ds *dataStore, seed uint64, ms *metadataStore) {
	var seedBytes [32]byte
	binary.LittleEndian.PutUint64(seedBytes[:8], seed)
	cc := rand.NewChaCha8(seedBytes)
	rng := rand.New(cc)

	buf := make([]byte, ms.maxChunkSize + prefixLen * 10)

	actions := []fuzzAction{
		{
			Name: "Read random chunk",
			Weight: 500,
			Run: func() error {
				ms.mu.Lock()

				hash, ci, ok := randItem(ms.Chunks, rng)
				if !ok {
					ms.mu.Unlock()
					return nil
				}

				ms.mu.Unlock()

				data, err := ds.Get(ci, buf)
				if err != nil {
					return fmt.Errorf("getting chunk: %w", err)
				}

				hashData := sha256.Sum256(data)

				if hashData != hash {
					return fmt.Errorf("chunk '%s', has data that does not match hash", hex.EncodeToString(hash[:])[:8])
				}

				return nil
			},
		},
		{
			Name: "Write random chunk",
			Weight: 50,
			Run: func() error {

				chunkSize := rng.Uint32N(ms.maxChunkSize - minMinChunkSize) + minMinChunkSize
				chunkBytes := buf[:chunkSize]

				if _, err := cc.Read(chunkBytes); err != nil {
					return fmt.Errorf("reading random bytes into buffer: %w", err)
				}

				ci, err := ds.Put(chunkBytes)
				if err != nil {
					return fmt.Errorf("putting chunk: %w", err)
				}

				hash := sha256.Sum256(chunkBytes)

				ms.mu.Lock()
				defer ms.mu.Unlock()

				if _, ok := ms.Chunks[hash]; ok {
					return nil
				}

				ms.Chunks[hash] = ci
				ms.packs[ci.PackID] = struct{}{}

				return nil
			},
		},
		{
			Name: "Delete random chunk",
			Weight: 50,
			Run: func() error {
				ms.mu.Lock()
				defer ms.mu.Unlock()

				hash, _, ok := randItem(ms.Chunks, rng)
				if !ok {
					return nil
				}

				delete(ms.Chunks, hash)

				return nil
			},
		},
		{
			Name: "Delete random pack",
			Weight: 5,
			Run: func() error {
				ms.mu.Lock()

				packID, _, ok := randItem(ms.packs, rng)
				if !ok {
					ms.mu.Unlock()
					return nil
				}

				for hash, ci := range ms.Chunks {
					if ci.PackID != packID {
						continue
					}
					delete(ms.Chunks, hash)
				}

				ms.mu.Unlock()

				
				if _, err := ds.DeletePacks([]uint64{packID}); err != nil {
					return fmt.Errorf("deleting packs: %w", err)
				}

				return nil
			},
		},
	}

	for range 1000 {
		if err := pickAndRun(actions, rng); err != nil {
			t.Fatal(err)
		}
	}
}

