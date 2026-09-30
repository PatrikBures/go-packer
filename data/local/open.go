package data

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

// Opens the next pack.
func (ds *dataStore) openPackWriteNext() (int64, error) {
	lock, packID, err := ds.lockActive(ds.nextPackID)
	if err != nil {
		return 0, fmt.Errorf("locking active: %w", err)
	}

	size, err := ds.createPack(packID, lock)
	if err != nil {
		return 0, fmt.Errorf("creating pack %d: %w", packID, err)
	}

	return size, nil
}

// Automatically opens the next snapshot.
//
// if there already are snapshots in active, it picks the largest unlocked pack.
//
// if there are no active snapshots and there is a completed dir,
// it finds the latest pack in completed and starts opening from that id +1.
//
// if there are no active and no completed packs, it starts opening packs from 0.
func (ds *dataStore) openPackWrite() (size int64, err error) {
	if err = os.Mkdir(ds.activeDir(), 0775); err != nil && !errors.Is(err, os.ErrExist) {
		return 0, fmt.Errorf("creating active dir: %w", err)
	}

	var lock *flock.Flock
	var packID uint64
	for range 5 {
		var err error
		time.Sleep(time.Millisecond * time.Duration(rand.Int64N(50)))

		lock, packID, err = ds.tryGetLockOfPack()
		if err != nil {
			return 0, fmt.Errorf("getting lock on pack: %w", err)
		}
		if lock != nil {
			break
		}
	}

	if lock == nil {
		return 0, fmt.Errorf("somehow did not manage to create/get a lock of a pack")
	}

	defer func() {
		if err == nil {
			return
		}
		err = errors.Join(err, lock.Close())
	}()

	return ds.createPack(packID, lock)
}

// Creates new pack, moves previous pack to completed and updates currentPack* vars
func (ds *dataStore) createPack(packID uint64, lock *flock.Flock) (size int64, err error) {

	f, err := os.OpenFile(ds.activePackPath(packID), os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return 0, fmt.Errorf("opening file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stating pack: %w", err)
	}
	size = stat.Size()
	if size == 0 {
		if _, err = writePackHeader(packID, stateActive, f); err != nil {
			return 0, fmt.Errorf("initializing pack: %w", err)
		}
		size = headerLen
	}

	// move previous open pack to completed
	if ds.currentPackLock != nil {
		p := ds.completedPackPath(ds.currentPackID)
		if err := os.MkdirAll(filepath.Dir(p), 0775); err != nil {
			return 0, fmt.Errorf("creating completed dir: %w", err)
		}

		var h header
		if err := h.ReadAtFile(ds.currentPackFile); err != nil {
			return 0, fmt.Errorf("reading header from prev pack: %w", err)
		}
		h.State = stateCompleted
		if err := h.WriteAtFile(ds.currentPackFile); err != nil {
			return 0, fmt.Errorf("writing header to prev pack: %w", err)
		}

		if err := ds.currentPackFile.Sync(); err != nil {
			return 0, fmt.Errorf("syncing prev pack: %w", err)
		}
		if err := ds.currentPackFile.Close(); err != nil {
			return 0, fmt.Errorf("closing prev pack: %w", err)
		}

		if err := os.Rename(ds.currentPackLock.Path(), p); err != nil {
			return 0, fmt.Errorf("renaming prev pack to completed: %w", err)
		}

		if err := ds.currentPackLock.Unlock(); err != nil {
			return 0, fmt.Errorf("unlocking prev pack: %w", err)
		}
	}

	ds.currentPackID = packID
	ds.currentPackLock = lock
	ds.currentPackFile = f
	ds.currentPackOffset = size

	return size, nil
}

// Gets file from cache. if it's not cached then opens the file and puts in cache.
func (ds *dataStore) openPackRead(packID uint64) (f *os.File, err error) {
	f = ds.packRead.Get(packID)
	if f != nil {
		return f, nil
	}

	f, err = os.OpenFile(ds.completedPackPath(packID), os.O_RDONLY, 0664)
	if errors.Is(err, os.ErrNotExist) {

		f, err = os.OpenFile(ds.activePackPath(packID), os.O_RDONLY, 0664)

		// we are checking again if it exist in completed as it could have been moved there between the 2 previous checks
		if errors.Is(err, os.ErrNotExist) {
			f, err = os.OpenFile(ds.completedPackPath(packID), os.O_RDONLY, 0664)
			if err != nil {
				return nil, fmt.Errorf("opening completed pack after failing to open completed and active: %w", err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("opening active pack: %w", err)
		}
	} else if err != nil {
		return nil, err
	}
	ds.packRead.Put(packID, f)

	return f, nil
}

func writePackHeader(packID uint64, state headerState, f *os.File) (header, error) {
	h := header{
		Magic:        defaultHeaderMagic,
		MajorVersion: majorVersion,
		MinorVersion: minorVersion,
		PackID:       packID,
		HeaderSize:   headerLen,
		State:        state,
	}
	_, err := h.WriteTo(f)
	return h, err
}
