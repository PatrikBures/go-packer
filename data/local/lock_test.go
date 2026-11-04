package data

import (
	"testing"
	"time"
)

func TestPackLockStaleLocked(t *testing.T) {
	ds := newDS(t,
		WithLockStaleDuration(time.Millisecond*30), 
		WithLockPulseInterval(time.Millisecond*10),
	)

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
		WithLockPulseInterval(time.Millisecond*1000),
	)

	if locked, err := ds.tryLockPack(0); err != nil {
		t.Fatal(err)
	} else if locked {
		t.Fatal("expected pack 0 to be locked")
	}

	time.Sleep(time.Millisecond*50)

	if locked, err := ds.tryLockPack(0); err != nil {
		t.Fatal(err)
	} else if !locked {
		t.Fatal("expected pack 0 to be unlocked")
	}
}
