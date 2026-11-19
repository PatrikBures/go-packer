package data

import (
	"cmp"
	"encoding/binary"
	"errors"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestPackLockStaleLocked(t *testing.T) {
	ds := newDS(t,
		WithLockStaleDuration(time.Millisecond*30), 
		WithLockPulseInterval(time.Millisecond*10),
	)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Error(err)
		}
	}()

	if locked, err := ds.tryLockPack(0); err != nil {
		t.Fatal(err)
	} else if locked {
		t.Fatal("expected pack 0 to be locked")
	}

	time.Sleep(time.Millisecond*50)

	if locked, err := ds.tryLockPack(0); err != nil {
		t.Fatal(err)
	} else if locked {
		t.Fatal("expected pack 0 to still be locked")
	}
}

func TestPackLockStaleUnlocked(t *testing.T) {
	ds := newDS(t,
		WithLockStaleDuration(time.Millisecond*30), 
		WithLockPulseInterval(time.Millisecond*10),
	)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Error(err)
		}
	}()

	if locked, err := ds.tryLockPack(0); err != nil {
		t.Fatal(err)
	} else if locked {
		t.Fatal("expected pack 0 to be locked")
	}

	ds.packPulse.Close()

	time.Sleep(time.Millisecond*50)

	if locked, err := ds.tryLockPack(0); err != nil {
		t.Fatal(err)
	} else if !locked {
		t.Fatal("expected pack 0 to be unlocked")
	}
}

func TestPackLockRemoved(t *testing.T) {
	ds := newDS(t,
		WithLockStaleDuration(time.Millisecond*30), 
		WithLockPulseInterval(time.Millisecond*10),
	)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Error(err)
		}
	}()

	if locked, err := ds.tryLockPack(0); err != nil {
		t.Fatal(err)
	} else if locked {
		t.Fatal("expected pack 0 to be locked")
	}

	if err := os.Remove(ds.activePackLockPath(0)); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(20 * time.Millisecond):
		t.Fatal("expected pulser to finish before ticker time ran out")
	case <-ds.packPulse.Done():
		want := os.ErrNotExist
		if err := ds.packPulse.Err(); !errors.Is(err, want) {
			t.Fatalf("expected pulser to fail with err '%v', received err '%v'", want, err)
		}
	}
}



func FuzzLockLargestPack(f *testing.F) {

	// when initializing ds, it creates a new pack with id 0.
	// that is why the loop starts from ID 1, to not interfere with it.
	f.Fuzz(func(t *testing.T, seed uint64) {
		ds := newDS(t, WithMaxPackSize(1024 << 30))
		list(t, ds.activeDir())

		var s [32]byte
		binary.LittleEndian.PutUint64(s[:], seed)
		r := rand.NewChaCha8(s)
		rng := rand.New(r)


		packQty := rng.Uint64N(14)+1
		maxSize := rng.Uint64N(8<<10-128) + 128 // max 8Ki
		chanceToBeLocked := rng.Float64()

		buf := make([]byte, maxSize)

		var largestAvailablePackSize uint64
		var largestAvailablePackID uint64
		var packAvailable bool

		for packID := uint64(1); packID < packQty; packID++ {
			isLocked := rng.ExpFloat64() <= chanceToBeLocked

			size := rng.Uint64N(maxSize)

			t.Logf("info: packID=%d, isLocked=%v, size=%d", packID, isLocked, size)
			if !isLocked && size > largestAvailablePackSize {
				t.Logf("updated available: packID=%d, size=%d", packID, size)
				largestAvailablePackSize = size
				largestAvailablePackID = packID
				packAvailable = true
			}
			createPack(t, ds, packID, isLocked, buf[:size])
		}

		locked, packID, err := ds.lockLargestPack(0)
		if err != nil {
			t.Fatalf("locking largest pack: %v", err)
		}
		if !locked && packAvailable {
			t.Fatal("pack is available but did not lock any")
		}

		if largestAvailablePackID != packID && packAvailable {
			// protecting agains the case that the selected pack has the same size
			// as the largest available one by only erroring if it is smaller.
			stat, err := os.Stat(ds.activePackPath(packID))
			if err != nil {
				t.Fatal(err)
			}
			if stat.Size() < int64(largestAvailablePackSize) {
				list(t, ds.activeDir())
				t.Fatalf("did not lock largest available pack: locked=%d, available=%d, available_size=%d", packID, largestAvailablePackID, largestAvailablePackSize)
			}
		}
	})
}

func TestLockLargestPack(t *testing.T) {
	ds := newDS(t)

	for i := range uint64(5) {
		// locked if uneven
		createPack(t, ds, i, (i & 1) == 0, []byte{})
	}

	list(t, ds.activeDir())

	locked, packID, err := ds.lockLargestPack(0)
	if err != nil {
		t.Errorf("locking largest pack: %v", err)
	}

	if locked {
		t.Logf("locked pack %d", packID)
	} else {
		t.Log("did not lock pack")
	}

	list(t, ds.activeDir())
}

func createPack(t *testing.T, ds *dataStore, packID uint64, locked bool, data []byte) {
	packPath := ds.activePackPath(packID)
	lockPath := ds.activePackLockPath(packID)

	if err := os.WriteFile(packPath, data, 0666); err != nil {
		t.Fatal(err)
	}

	extra := ""
	if locked {
		extra = "locked "
		if err := os.WriteFile(lockPath, []byte{}, 0666); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("created %spack %d", extra, packID)
}

type file struct {
	name string
	size int64
	packID uint64
}

func list(t *testing.T, dir string) {

	entries := make([]file, 0, 1000)
	lockedPacks := make(map[string]struct{}, 500)

	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		if strings.HasSuffix(d.Name(), ".lock") {
			name := strings.TrimSuffix(d.Name(), ".lock")
			lockedPacks[name] = struct{}{}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		packID, err := packIdFromPath(d.Name())
		if err != nil {
			return err
		}

		entries = append(entries, file{
			name: d.Name(),
			size: info.Size(),
			packID: packID,
		})

		return nil
	}); err != nil {
		t.Error(err)
	}

	t.Log()
	t.Logf("Listing files in %s", dir)

	slices.SortFunc(entries, func(a, b file) int {
		return cmp.Compare(a.packID, b.packID)
	})

	for _, f := range entries {
		lockByte := byte('-')
		if _, exists := lockedPacks[f.name]; exists {
			lockByte = byte('!')
		}

		t.Logf("%s % 8d pack-%d", string(lockByte), f.size, f.packID)
	}

	t.Log()
}
