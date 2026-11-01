package data

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
	"go.pabu.dev/packer"
)

func (ds *dataStore) SwitchPack(packID uint64) error {
	lock := flock.New(ds.activePackPath(packID))
	locked, err := lock.TryLock()
	if err != nil {
		return fmt.Errorf("locking pack: %w", err)
	}
	if !locked {
		_ = lock.Close()
		return fmt.Errorf("pack is already locked")
	}
	return ds.createPack(packID, lock)
}

func (ds *dataStore) SwitchPackExisting() (err error) {

	lock, packID, err := ds.lockLargestPack()
	if err != nil {
		return fmt.Errorf("locking largest pack: %w", err)
	}
	if lock == nil {
		return packer.ErrNewPack
	}

	defer func() {
		if err == nil { return }
		_ = lock.Unlock()
	}()

	f, err := os.OpenFile(ds.activePackPath(packID), os.O_RDWR, 0664)
	if err != nil {
		return fmt.Errorf("opening existing pack: %w", err)
	}

	defer func() {
		if err == nil { return }
		_ = f.Close()
	}()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stating pack: %w", err)
	}

	if err = ds.movePackCompleted(); err != nil {
		return fmt.Errorf("completing pack: %w", err)
	}

	ds.currentPackID = packID
	ds.currentPackLock = lock
	ds.currentPackFile = f
	ds.currentPackOffset = stat.Size()

	return nil
}

// Creates new pack, moves previous pack to completed and updates currentPack* vars
func (ds *dataStore) createPack(packID uint64, lock *flock.Flock) error {

	f, err := os.OpenFile(ds.activePackPath(packID), os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stating pack: %w", err)
	}
	size := stat.Size()
	if size == 0 {
		if _, err = writePackHeader(packID, stateActive, f); err != nil {
			return fmt.Errorf("initializing pack: %w", err)
		}
		size = headerLen
	}

	if err := ds.movePackCompleted(); err != nil {
		return err
	}

	ds.currentPackID = packID
	ds.currentPackLock = lock
	ds.currentPackFile = f
	ds.currentPackOffset = size

	return nil
}

// moves current pack to completed, closes, unlocks it and updates header state.
func (ds *dataStore) movePackCompleted() error {
	if ds.currentPackLock == nil {
		return nil
	}

	p := ds.completedPackPath(ds.currentPackID)
	if err := os.MkdirAll(filepath.Dir(p), 0775); err != nil {
		return fmt.Errorf("creating completed dir: %w", err)
	}

	var h header
	if err := h.ReadAtFile(ds.currentPackFile); err != nil {
		return fmt.Errorf("reading header from prev pack: %w", err)
	}
	h.State = stateCompleted
	if err := h.WriteAtFile(ds.currentPackFile); err != nil {
		return fmt.Errorf("writing header to prev pack: %w", err)
	}

	if err := ds.currentPackFile.Sync(); err != nil {
		return fmt.Errorf("syncing prev pack: %w", err)
	}
	if err := ds.currentPackFile.Close(); err != nil {
		return fmt.Errorf("closing prev pack: %w", err)
	}

	if err := os.Rename(ds.currentPackLock.Path(), p); err != nil {
		return fmt.Errorf("renaming prev pack to completed: %w", err)
	}

	if err := ds.currentPackLock.Unlock(); err != nil {
		return fmt.Errorf("unlocking prev pack: %w", err)
	}

	return nil
}

// Gets file from cache. if it's not cached then opens the file and puts in cache.
func (ds *dataStore) openPackRead(packID uint64) (f *os.File, err error) {
	f = ds.packRead.Get(packID)
	if f != nil {
		return f, nil
	}

	paths := []string{
		ds.completedPackPath(packID),
		ds.activePackPath(packID),
		ds.completedPackPath(packID),
	}

	for i, p := range paths {
		f, err = os.OpenFile(p, os.O_RDONLY, 0664)

		if i < len(paths)-1 && errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("opening read pack %s: %w", paths[i], err)
		}
		break
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
