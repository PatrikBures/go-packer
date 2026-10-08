package packertest

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
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

	rng := rand.New(rand.NewPCG(conf.Seed, 0))

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

	for _, p := range packagers {
		if err := p.Close(); err != nil {
			t.Fatalf("closing packager: %w", err)
		}
	}
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
			Weight: 200,
			Run: func() error {
				ids, err := p.ChunkIDsAll()
				if err != nil {
					return err
				}
				if len(ids) == 0 {
					return nil
				}

				l := len(ids)
				if l > 500 {
					l = 500
				}

				idx := rng.IntN(l) + (len(ids) - l)
				hash := ids[idx]

				data, err := p.Get(hash, buf)
				if err != nil {
					return fmt.Errorf("getting chunk from packer: %w", err)
				}

				hashReceived := sha256.Sum256(data)

				if !slices.Equal(hash, hashReceived[:]) {
					l := len(ids)
					if l > 3 {
						l = 3
					}

					idsString := make([]string, 0, l)
					for _, id := range ids[:l] {
						idsString = append(idsString, fmt.Sprintf("%x", id))
					}
					idsStringSep := strings.Join(idsString, "\n")
					return fmt.Errorf("content of hash %x does not match, received: %x, idx: %d, len: %d\nids:\n%s", hash, hashReceived, idx, len(ids), idsStringSep)
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
		{
			Name: "Delete bottom chunks",
			Weight: 10,
			Run: func() error {
				ids, err := p.ChunkIDsAll()
				if err != nil {
					return err
				}
				if len(ids) == 0 {
					return nil
				}

				limit := 1000
				if len(ids) < limit {
					return nil
				}

				deleteIds := ids[:len(ids)-1000]

				return p.Delete(deleteIds)
			},
		},
		{
			Name: "Compact",
			Weight: 1,
			Run: func() error {
				_, err := p.Compact()
				return err
			},
		},
	}


	for range actionQty {
		if err := PickAndRunAction(actions, rng); err != nil {
			t.Error(err)
		}
	}
}
