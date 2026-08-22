package local

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
func (ds *dataStore) openPackWriteNext() (size int64, err error) {
	packID := ds.currentPackID+1
	lock, err := ds.lockActive(packID)
	return ds.createPack(packID, lock)
}

func (ds *dataStore) openPackCompact() (*os.File, error) {
	return nil, nil

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
	for range 5 {
		var err error
		time.Sleep(time.Millisecond * time.Duration(rand.Int64N(50)))

		lock, err = ds.tryGetLockOfPack()
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
		if err == nil { return }
		err = errors.Join(err, lock.Close())
	}()

	p := lock.Path()
	packID, err := packIdFromPath(p)
	if err != nil {
		return 0, fmt.Errorf("getting pack from path: %w", err)
	}

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
		if err = initPack(packID, f); err != nil {
			return 0, fmt.Errorf("initializing pack: %w", err)
		}
		size = headerLen
	}

	if ds.currentPackLock != nil {
		p := ds.completedPackPath(ds.currentPackID)
		if err := os.MkdirAll(filepath.Dir(p), 0775); err != nil {
			return 0, fmt.Errorf("creating completed dir: %w", err)
		}
		if err := os.Rename(ds.currentPackLock.Path(), p); err != nil {
			return 0, fmt.Errorf("renaming pack to completed: %w", err)
		}
	}

	ds.currentPackID = packID
	ds.currentPackLock = lock
	ds.currentPackFile = f
	ds.currentPackOffset.Store(size)

	return size, nil
}


func (ds *dataStore) openPackRead(packID uint64) (*os.File, error) {
	if packID == ds.currentPackID {
		return ds.currentPackFile, nil
	}
	f := ds.packRead.Get(packID)
	if f != nil {
		return f, nil
	}
	
	f, err := os.OpenFile(ds.completedPackPath(packID), os.O_RDONLY, 0664)
	if err != nil {
		return nil, err
	}
	ds.packRead.Put(packID, f)

	return f, nil
}


func initPack(packID uint64, f *os.File) error {
	h := header{
		Magic:   defaultHeaderMagic,
		MajorVersion: majorVersion,
		MinorVersion: minorVersion,
		PackID:  packID,
		HeaderSize: headerLen,
		State: stateActive,
	}
	_, err := h.WriteTo(f)
	return err
}
