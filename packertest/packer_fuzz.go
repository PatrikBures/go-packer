package packertest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.pabu.dev/bytesize"
	"go.pabu.dev/packer"
)

var argInstances int
var argRoutinesPerInstance int
var argDuration time.Duration
var argSeed uint64

func init() {
	flag.IntVar(&argInstances,           "instances", 0,                "Number of packer instances")
	flag.IntVar(&argRoutinesPerInstance, "routines",  0,                "Number of routines running on each instance")
	flag.DurationVar(&argDuration,       "duration",  time.Millisecond, "How long the test should run")
	flag.Uint64Var(&argSeed,             "seed",      0,                "Seed used to generate random data")
}

type RunRandomActionsConfig struct {
	Instances           int
	RoutinesPerInstance int
	Duration            time.Duration
	Seed                uint64
	InitPackager        func(dir string, maxPackSize int) (*packer.Packager, error)
}

func RunRandomActions(t *testing.T, conf RunRandomActionsConfig) {
	root := tempDir(t)

	if argInstances != 0 { conf.Instances = argInstances }
	if argRoutinesPerInstance != 0 { conf.RoutinesPerInstance = argRoutinesPerInstance }
	if argDuration >= time.Second { conf.Duration = argDuration }
	if argSeed != 0 { conf.Seed = argSeed }

	// extra instance for the compactor, if there will be multiple instances.
	// otherwise the compactor will share the one and only instance.
	totalInstances := conf.Instances
	if totalInstances > 1 {
		totalInstances++
	}

	t.Log("=== Running random actions")
	t.Log("  - Root dir:           ", root)
	t.Log("  - Instances:          ", conf.Instances)
	t.Log("  - Instances Total:    ", totalInstances)
	t.Log("  - RoutinesPerInstance:", conf.RoutinesPerInstance)
	t.Log("  - Duration:           ", conf.Duration.String())
	t.Log("  - Seed:               ", conf.Seed)
	t.Log()

	rng := rand.New(rand.NewPCG(conf.Seed, 0))

	maxPackSize := rng.IntN(MaxMaxPackSize-MinMinPackSize) + MinMinPackSize
	maxChunkSize := rng.Uint32N(MaxMaxChunkSize-MinMinChunkSize) + MinMinChunkSize + 1

	packagers := make([]*packer.Packager, 0, totalInstances)
	for i := range totalInstances {
		p, err := conf.InitPackager(root, maxPackSize)
		if err != nil {
			t.Fatalf("initializing packager idx %d: %v", i, err)
		}
		packagers = append(packagers, p)
	}

	deadline := time.Now().Add(conf.Duration)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()

	var wg sync.WaitGroup
	for pi, p := range packagers[:conf.Instances] {
		t.Log("=== Starting packager", pi+1)

		for ri := range conf.RoutinesPerInstance {
			t.Log("  - Starting routine", ri+1)
			subSeed := rng.Uint64()

			wg.Go(func() {
				RunRoutineActions(t, ctx, cancel, subSeed, p, maxChunkSize)
			})
		}
	}

	t.Log("=== Starting compactor")
	wg.Go(func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var p *packer.Packager
		if totalInstances == 1 {
			p = packagers[0]
		} else {
			p = packagers[totalInstances-1]
		}

		for {
			exit := false

			select {
			case <-ctx.Done():
				exit = true
			case <-ticker.C:
			}
			info, err := p.Compact()
			if err != nil {
				t.Errorf("compacting: %v", err)
				cancel()
				return
			}
			t.Logf("compacted: %s", info)

			if exit {
				return
			}

		}
	})

	wg.Wait()

	defer func() {
		for _, p := range packagers {
			if err := p.Close(); err != nil {
				t.Errorf("closing packager: %v", err)
			}
		}
	}()

	var physicalBytes int64
	for _, dir := range []string{"active", "completed"} {
		filepath.Walk(filepath.Join(root, dir), func(path string, info fs.FileInfo, err error) error {
			if err != nil { return err }
			if info.IsDir() || !strings.HasSuffix(path, ".bin") { return nil }
			physicalBytes += info.Size()
			return nil
		})
	}

	var liveBytes uint64
	info, err := packagers[0].PackInfoAll()
	if err != nil {
		t.Fatalf("reading pack info: %v", err)
	}
	for _, pi := range info {
		liveBytes += pi.Size
	}

	ratio := float64(liveBytes) / float64(physicalBytes)

	t.Logf("live=%s, physical=%s, ratio=%.3f", bytesize.FormatIEC(liveBytes), bytesize.FormatIEC(physicalBytes), ratio)
	if ratio < 0.3 {
		t.Errorf("compaction ineffective: live/physical ratio=%.3f < 0.3", ratio)
	}
}

func RunRoutineActions(t *testing.T, ctx context.Context, cancel context.CancelFunc, seed uint64, p *packer.Packager, maxChunkSize uint32) {
	var seedBytes [32]byte
	binary.LittleEndian.PutUint64(seedBytes[:8], seed)
	cc := rand.NewChaCha8(seedBytes)
	rng := rand.New(cc)

	buf := make([]byte, maxChunkSize+1024)

	actions := []FuzzAction{
		{
			Name:   "Read random chunk",
			Weight: 20000,
			Run: func() error {
				ids, err := p.ChunkIDsAll()
				if err != nil {
					return err
				}
				if len(ids) == 0 {
					return nil
				}

				if len(ids) > 500 {
					ids = ids[len(ids)-500:]
				}

				ids = ids[len(ids)>>1:]

				idx := rng.IntN(len(ids))
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
			Name:   "Write random chunk",
			Weight: 5000,
			Run: func() error {

				chunkSize := rng.Uint32N(maxChunkSize-MinMinChunkSize+1) + MinMinChunkSize
				chunkBytes := buf[:chunkSize]

				if _, err := cc.Read(chunkBytes); err != nil {
					return fmt.Errorf("reading random bytes into buffer: %w", err)
				}

				hash := sha256.Sum256(chunkBytes)

				err := p.Put(hash[:], chunkBytes)
				if err != nil {
					return fmt.Errorf("putting chunk: %w", err)
				}

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
			Name:   "Delete bottom chunks",
			Weight: 5,
			Run: func() error {
				ids, err := p.ChunkIDsAll()
				if err != nil {
					return err
				}
				if len(ids) == 0 {
					return nil
				}

				if len(ids) < 1000 {
					return nil
				}

				deleteIds := ids[:len(ids)-1000]

				return p.Delete(deleteIds)
			},
		},
	}

	for ctx.Err() == nil {
		if err := PickAndRunAction(actions, rng); err != nil {
			t.Error(err)
			cancel()
			return
		}
	}
}
