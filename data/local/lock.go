package data

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/gofrs/flock"
)

func (ds *dataStore) tryGetLockOfPack() (*flock.Flock, uint64, error) {
	// there is a pack in active dir
	lock, packID, err := ds.lockLargestPack()
	if err != nil {
		return nil, 0, fmt.Errorf("locking largest pack: %w", err)
	}
	if lock != nil {
		return lock, packID, nil
	}

	// completed dir does not exist
	lock, packID, err = ds.lockNoComplete()
	if err != nil {
		return nil, 0, fmt.Errorf("locking with no complete dir: %w", err)
	}
	if lock != nil {
		return lock, packID, nil
	}

	// completed dir exists
	lock, packID, err = ds.lockLatestFromComplete()
	if err != nil {
		return nil, 0, fmt.Errorf("locking from latest pack: %w", err)
	}
	if lock != nil {
		return lock, packID, nil
	}

	return nil, 0, nil
}

// acquires lock if complete dir does not exist
func (ds *dataStore) lockNoComplete() (*flock.Flock, uint64, error) {

	stat, err := os.Stat(ds.completedDir())
	if errors.Is(err, os.ErrNotExist) {
		lock, packID, err := ds.lockActive(0)
		if err != nil {
			return nil, 0, err
		}
		if lock != nil {
			return lock, packID, nil
		}
	} else if err != nil {
		return nil, 0, err
	}
	if !stat.IsDir() {
		return nil, 0, fmt.Errorf("active dir is not a dir")
	}

	return nil, 0, nil
}

// packIDstart is the first id it will try to lock.
// Checks if pack already does not exist in completed.
func (ds *dataStore) lockActive(packIDstart uint64) (*flock.Flock, uint64, error) {
	for packID := packIDstart; packID < packIDstart+100; packID++ {
		_, err := os.Stat(ds.completedPackPath(packID))
		if err == nil {
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, 0, fmt.Errorf("checking if pack already exists in completed: %w", err)
		}

		lock := flock.New(ds.activePackPath(packID))

		locked, err := lock.TryLock()
		if err != nil {
			return nil, 0, fmt.Errorf("trying to get lock: %w", err)
		}

		// if it did not manage to lock, try next id
		if !locked {
			if err := lock.Close(); err != nil {
				return nil, 0, fmt.Errorf("closing lock: %w", err)
			}
			continue
		}

		_, err = os.Stat(ds.completedPackPath(packID))

		// returns only if the completed pack path does not exist
		// it is checking again because it could happen that between checking the first
		// time and locking that it got created in completed by another instance/goroutine
		if errors.Is(err, os.ErrNotExist) {

			// increases nextPackID only if it is higher than the previous one
			if packID <= ds.nextPackID {
				ds.nextPackID++
			}

			return lock, packID, nil

		} else if err != nil {
			_ = lock.Close()
			_ = os.Remove(lock.Path())
			return nil, 0, fmt.Errorf("stating completed: %w", err)
		}

		// clean up the created file in active
		if err := os.Remove(lock.Path()); err != nil {
			return nil, 0, fmt.Errorf("removing active file after finding out it already exists in completed: %w", err)
		}
		if err := lock.Close(); err != nil {
			return nil, 0, fmt.Errorf("unlocking active file after finding out it already exists in completed: %w", err)
		}
	}

	return nil, 0, ErrLimitMaxActive
}

// finds the latest pack in completed dir, and tries to lock any id starting from that
func (ds *dataStore) lockLatestFromComplete() (*flock.Flock, uint64, error) {
	var latestDir string
	if err := filepath.WalkDir(ds.completedDir(), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		latestDir = path
		return nil
	}); err != nil {
		return nil, 0, err
	}

	var latestPack string
	if err := filepath.WalkDir(latestDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		latestPack = d.Name()
		return nil
	}); err != nil {
		return nil, 0, err
	}


	latestPackID, err := packIdFromPath(latestPack)
	if err != nil {
		return nil, 0, err
	}

	nextPackID := latestPackID + 1

	return ds.lockActive(nextPackID)
}

type fileItem struct {
	name string
	size int64
}

// if flock and err are nil, it means that no active pack was unlocked
func (ds *dataStore) lockLargestPack() (*flock.Flock, uint64, error) {

	files, err := sortedFilesLargestDesc(ds.activeDir())
	if err != nil {
		return nil, 0, err
	}

	if len(files) == 0 {
		return nil, 0, nil
	}

	var locked bool
	var lock *flock.Flock
	for _, f := range files {
		fp := filepath.Join(ds.activeDir(), f.name)

		lock = flock.New(fp)

		locked, err = lock.TryLock()
		if err != nil {
			return nil, 0, fmt.Errorf("trying locking file: %w", err)
		}

		if locked {
			break
		}

		if err := lock.Close(); err != nil {
			return nil, 0, fmt.Errorf("closing lock: %w", err)
		}
	}

	if !locked {
		return nil, 0, nil
	}

	packID, err := packIdFromPath(lock.Path())
	if err != nil {
		return nil, 0, err
	}

	return lock, packID, nil
}

func sortedFilesLargestDesc(dir string) ([]fileItem, error) {
	var files []fileItem

	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		files = append(files, fileItem{
			name: info.Name(),
			size: info.Size(),
		})

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking dir: %w", err)
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].size > files[j].size
	})

	return files, nil
}
