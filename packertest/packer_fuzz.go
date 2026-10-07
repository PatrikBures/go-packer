package packertest

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"go.pabu.dev/packer"
)


type RunRandomActionsConfig struct {
	Instances           int
	RoutinesPerInstance int
	ActionQty           int
	Seed                uint64
	InitPackager        func(dir string, maxPackSize int) (*packer.Packager, error)
}

func RunRandomActions(t *testing.T, conf RunRandomActionsConfig) {
	root := t.TempDir()

	var seedBytes [32]byte
	binary.LittleEndian.PutUint64(seedBytes[:8], conf.Seed)
	cc := rand.NewChaCha8(seedBytes)
	rng := rand.New(cc)

	maxPackSize  := rng.IntN(MaxMaxPackSize - MinMinPackSize) + MinMinPackSize
	maxChunkSize := rng.Uint32N(MaxMaxChunkSize - MinMinChunkSize) + MinMinChunkSize +1

	packagers := make([]*packer.Packager, 0, conf.Instances)
	for i := range conf.Instances {
		p, err := conf.InitPackager(root, maxPackSize)
		if err != nil {
			t.Fatalf("initializing packager idx %d: %v", i, err)
		}
		packagers = append(packagers, p)
	}
	var wg sync.WaitGroup
	for _, p := range packagers {
		for range conf.RoutinesPerInstance {
			subSeed := rng.Uint64()
			wg.Add(1)

			go func() {
				defer wg.Done()

				defer func() {
					if r := recover(); r != nil {
						t.Errorf("goroutine panicked: %v", r)
					}
				}()

				RunRoutineActions(t, subSeed, p, maxChunkSize, conf.ActionQty)
			}()
		}
	}

	wg.Wait()
}

func RunRoutineActions(t *testing.T, seed uint64, p *packer.Packager, maxChunkSize uint32, actionQty int) {
	var seedBytes [32]byte
	binary.LittleEndian.PutUint64(seedBytes[:8], seed)
	cc := rand.NewChaCha8(seedBytes)
	rng := rand.New(cc)

	buf := make([]byte, maxChunkSize + 1024)

	actions := []FuzzAction{
		{
			Name: "Read random chunk",
			Weight: 500,
			Run: func() error {
				ids, err := p.ChunkIDsAll()
				if err != nil {
					return err
				}
				if len(ids) == 0 {
					return nil
				}

				fourth := len(ids) >> 2
				if fourth < 1 {
					fourth = 1
				}
				add := fourth * 3
				if add > len(ids)-1 {
					add = len(ids)-1
				}

				idx := rng.IntN(fourth) + (add)
				hash := ids[idx]

				data, err := p.Get(hash, buf)
				if err != nil {
					return fmt.Errorf("getting chunk from packer: %w", err)
				}

				hashReceived := sha256.Sum256(data)

				if !slices.Equal(hash, hashReceived[:]) {
					idsString := make([]string, 0, len(ids))
					for _, id := range ids {
						idsString = append(idsString, fmt.Sprintf("%x", id))
					}
					return fmt.Errorf("content of hash %x does not match, received: %x, idx: %d, len: %d\nids: %+v", hash, hashReceived, idx, len(ids), idsString)
				}

				return nil
			},
		},
		{
			Name: "Write random chunk",
			Weight: 50,
			Run: func() error {

				chunkSize := rng.Uint32N(maxChunkSize - MinMinChunkSize+1) + MinMinChunkSize
				chunkBytes := buf[:chunkSize]

				if _, err := cc.Read(chunkBytes); err != nil {
					return fmt.Errorf("reading random bytes into buffer: %w", err)
				}

				hash := sha256.Sum256(chunkBytes)

				p.Put(hash[:], chunkBytes)

				chunkBytesGet, err := p.Get(hash[:], buf)
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


	for range actionQty {
		if err := PickAndRunAction(actions, rng); err != nil {
			t.Error(err)
		}
	}
}
