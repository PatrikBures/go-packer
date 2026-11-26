package data

import (
	"errors"
	"fmt"
	"os"

	"go.pabu.dev/packer"
	"go.pabu.dev/packer/data/local/internal/pulser"
)

func (ds *dataStore) SwitchPack(packID uint64) error {
	ds.writePackMu.Lock()
	defer ds.writePackMu.Unlock()

	locked, err := ds.tryLockPack(packID)
	if err != nil {
		return fmt.Errorf("locking pack: %w", err)
	}
	if !locked {
		return fmt.Errorf("pack is already locked")
	}

	if err := ds.createPack(packID); err != nil {
		return err
	}

	return nil
}

func (ds *dataStore) SwitchPackExisting(minSize int64) (err error) {
	ds.writePackMu.Lock()
	defer ds.writePackMu.Unlock()

	locked, packID, err := ds.lockLargestPack(minSize)
	if err != nil {
		return fmt.Errorf("locking largest pack: %w", err)
	}
	if !locked {
		return packer.ErrNewPack
	}

	defer func() {
		if err == nil { return }
		if uerr := ds.unlockPack(packID); uerr != nil {
			err = errors.Join(err, fmt.Errorf("unlocking after error: %w", uerr))
		}
	}()

	if _, err = os.Stat(ds.activePackPath(packID)); err != nil {
		return fmt.Errorf("bug! pack dissapeared after locking")
	}

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

	if ds.currentPackFile != nil && !ds.packWrite.Exists(ds.currentPackID) {
		if err := ds.movePackCompleted(ds.currentPackID, ds.currentPackFile, ds.packPulse); err != nil {
			return fmt.Errorf("moving previous pack to completed: %w", err)
		}
	}

	ds.packPulse = ds.startPackPulse(packID)
	ds.currentPackID = packID
	ds.currentPackFile = f
	ds.currentPackOffset = stat.Size()

	return nil
}

// Creates new pack, moves previous pack to completed and updates currentPack* vars
func (ds *dataStore) createPack(packID uint64) (err error) {
	defer func() {
		if err == nil { return }
		if uerr := ds.unlockPack(packID); uerr != nil {
			err = errors.Join(err, fmt.Errorf("unlocking after error: %w", uerr))
		}
	}()

	f, err := os.OpenFile(ds.activePackPath(packID), os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}

	defer func() {
		if err == nil { return }
		_ = f.Close()
	}()

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

	if ds.currentPackFile != nil && !ds.packWrite.Exists(ds.currentPackID) {
		if err := ds.movePackCompleted(ds.currentPackID, ds.currentPackFile, ds.packPulse); err != nil {
			return fmt.Errorf("moving previous pack to completed: %w", err)
		}
	}

	ds.packPulse = ds.startPackPulse(packID)
	ds.currentPackID = packID
	ds.currentPackFile = f
	ds.currentPackOffset = size

	return nil
}

// closes pack, stops pulse, updates header state, unlocks and moves pack to completed.
func (ds *dataStore) movePackCompleted(packID uint64, f *os.File, pulse *pulser.Pulse) error {

	if err := os.MkdirAll(ds.completedPackDirPath(packID), 0775); err != nil {
		return fmt.Errorf("creating completed dir: %w", err)
	}

	var h header
	if err := h.ReadAtFile(f); err != nil {
		return fmt.Errorf("reading header from prev pack: %w", err)
	}
	h.State = stateCompleted
	if err := h.WriteAtFile(f); err != nil {
		return fmt.Errorf("writing header to prev pack: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing prev pack: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing prev pack: %w", err)
	}

	if err := os.Rename(ds.activePackPath(packID), ds.completedPackPath(packID)); err != nil {
		return fmt.Errorf("renaming prev pack to completed: %w", err)
	}

	pulse.Close()

	if err := ds.unlockPack(packID); err != nil {
		return fmt.Errorf("unlocking pack that was moved to completed: %w", err)
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
