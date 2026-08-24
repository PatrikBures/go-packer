package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/gofrs/flock"
)

func (ds *dataStore) tryGetLockOfPack() (*flock.Flock, error) {
	// there is a pack in active dir
	lock, err := ds.lockLargestPack()
	if err != nil {
		return nil, fmt.Errorf("locking largest pack: %w", err)
	}
	if lock != nil {
		return lock, nil
	}

	// completed dir does not exist
	lock, err = ds.lockNoComplete()
	if err != nil {
		return nil, fmt.Errorf("locking with no complete dir: %w", err)
	}
	if lock != nil {
		return lock, nil
	}

	// completed dir exists
	lock, err = ds.lockLatestFromComplete()
	if err != nil {
		return nil, fmt.Errorf("locking from latest pack: %w", err)
	}
	if lock != nil {
		return lock, nil
	}

	return nil, nil
}

// acquires lock if complete dir does not exist
func (ds *dataStore) lockNoComplete() (*flock.Flock, error) {

	stat, err := os.Stat(ds.completedDir())
	if errors.Is(err, os.ErrNotExist) {
		lock, err := ds.lockActive(0)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			return lock, nil
		}
	} else if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("active dir is not a dir")
	}

	return nil, nil
}

// packIDstart is the first id it will try to lock.
// Checks if pack already does not exist in completed.
func (ds *dataStore) lockActive(packIDstart uint64) (*flock.Flock, error) {
	for i := packIDstart; i < packIDstart+100; i++ {

		lock := flock.New(ds.activePackPath(i))

		locked, err := lock.TryLock()
		if err != nil {
			return nil, err
		}

		if locked {
			_, err := os.Stat(ds.completedPackPath(i))

			// returns only if the completed pack path does not exist
			if errors.Is(err, os.ErrNotExist) {
				return lock, nil
			} else if err != nil {
				_ = lock.Close()
				return nil, fmt.Errorf("stating completed: %w", err)
			}
		}

		if err := lock.Close(); err != nil {
			return nil, err
		}
	}

	return nil, ErrLimitMaxActive
}

// finds the latest pack in completed dir, and tries to lock any id starting from that
func (ds *dataStore) lockLatestFromComplete() (*flock.Flock, error) {
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
		return nil, err
	}

	fmt.Println("latestDir:", latestDir)

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
		return nil, err
	}

	fmt.Println("latestPack:", latestPack)

	latestPackID, err := packIdFromPath(latestPack)
	if err != nil {
		return nil, err
	}

	nextPackID := latestPackID + 1
	fmt.Println("latestPackID:", latestPackID)
	fmt.Println("nextPackID:", nextPackID)

	return ds.lockActive(nextPackID)
}

type fileItem struct {
	name string
	size int64
}

// if flock and err are nil, it means that no active pack was unlocked
func (ds *dataStore) lockLargestPack() (*flock.Flock, error) {

	files, err := sortedFilesLargestDesc(ds.activeDir())
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, nil
	}

	var locked bool
	var lock *flock.Flock
	for _, f := range files {
		fp := filepath.Join(ds.activeDir(), f.name)

		lock = flock.New(fp)

		locked, err = lock.TryLock()
		if err != nil {
			return nil, fmt.Errorf("trying locking file: %w", err)
		}

		if locked {
			break
		}

		if err := lock.Close(); err != nil {
			return nil, fmt.Errorf("closing lock: %w", err)
		}
	}

	if !locked {
		return nil, nil
	}

	return lock, nil
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
