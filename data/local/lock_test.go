package data

import (
	"errors"
	"os"
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
