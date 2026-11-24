package packertest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
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

var argCompactInterval time.Duration
var argTempDir string

var argMaxChunkSize string
var argMaxPackSize string

func init() {
	flag.IntVar(&argInstances,            "instances",         0,                "Number of packer instances")
	flag.IntVar(&argRoutinesPerInstance,  "routines",          0,                "Number of routines running on each instance")
	flag.DurationVar(&argDuration,        "duration",          time.Millisecond, "How long the test should run")
	flag.Uint64Var(&argSeed,              "seed",              0,                "Seed used to generate random data")

	flag.DurationVar(&argCompactInterval, "compact-interval",  time.Second*10,   "How often a compaction should occur")
	flag.StringVar(&argTempDir,           "temp-dir",          "",               "The dir the test should use for the test")

	flag.StringVar(&argMaxChunkSize,      "max-chunk-size",    "",               "maximum random chunk size")
	flag.StringVar(&argMaxPackSize,       "max-pack-size",     "",               "maximum random pack size")
}

type RunRandomActionsConfig struct {
	Instances           int
	RoutinesPerInstance int
	Duration            time.Duration
	Seed                uint64
	InitPackager        func(dir string, maxPackSize int) (*packer.Packager, error)
}

func RunRandomActions(t *testing.T, conf RunRandomActionsConfig) {
	var root string
	if argTempDir == "" {
		root = tempDir(t)
	} else {
		root = argTempDir
	}

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


	rng := rand.New(rand.NewPCG(conf.Seed, 0))

	var maxChunkSize uint32
	if argMaxChunkSize != "" {
		size, err := bytesize.Parse(argMaxChunkSize)
		if err != nil { t.Fatal(err) }
		maxChunkSize = uint32(size)
	} else {
		maxChunkSize = rng.Uint32N(MaxMaxChunkSize-MinMinChunkSize) + MinMinChunkSize + 1
	}

	var maxPackSize int
	if argMaxPackSize != "" {
		size, err := bytesize.Parse(argMaxPackSize)
		if err != nil { t.Fatal(err) }
		maxPackSize = int(size)
	} else {
		maxPackSize = rng.IntN(MaxMaxPackSize-MinMinPackSize) + MinMinPackSize
	}

	t.Log("=== Running random actions")
	fmt.Fprintln(t.Output(), "  - Root dir:           ", root)
	fmt.Fprintln(t.Output(), "  - Instances:          ", conf.Instances)
	fmt.Fprintln(t.Output(), "  - Instances Total:    ", totalInstances)
	fmt.Fprintln(t.Output(), "  - RoutinesPerInstance:", conf.RoutinesPerInstance)
	fmt.Fprintln(t.Output(), "  - Duration:           ", conf.Duration.String())
	fmt.Fprintln(t.Output(), "  - Seed:               ", conf.Seed)
	fmt.Fprintln(t.Output(), "  - CompactInterval:    ", argCompactInterval.String())
	fmt.Fprintln(t.Output(), "  - MaxChunkSize:       ", bytesize.FormatIEC(maxChunkSize))
	fmt.Fprintln(t.Output(), "  - MaxPackSize:        ", bytesize.FormatIEC(maxPackSize))

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
	for _, p := range packagers[:conf.Instances] {
		for range conf.RoutinesPerInstance {
			subSeed := rng.Uint64()

			wg.Go(func() {
				RunRoutineActions(t, ctx, cancel, subSeed, p, maxChunkSize)
			})
		}
	}

	t.Log("=== Starting compactor")
	wg.Go(func() {
		ticker := time.NewTicker(argCompactInterval)
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

				// returns without compacting if it canceled, meaning 
				// something wrong occured so the state is not modified after.
				if !errors.Is(ctx.Err(), context.DeadlineExceeded){
					return
				}
			case <-ticker.C:
			}
			start := time.Now()
			info, err := p.Compact()
			if err != nil {
				t.Errorf("compacting: %v", err)
				cancel()
				return
			}
			t.Logf("compacted in %s: %s", time.Now().Sub(start).Round(time.Microsecond*10), info)

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

				pctx, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*2000))
				err := p.Put(pctx, hash[:], chunkBytes)
				if err != nil {
					return fmt.Errorf("putting chunk: %w", err)
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
