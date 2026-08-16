package local

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/gofrs/flock"
)

func (ds *dataStore) tryGetLockOfPack() (*flock.Flock, error) {
	// there is a pack in active dir
	lock, err := lockLargestPack(ds.packDir)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		return lock, nil
	}

	// completed dir does not exist
	lock, err = lockNoComplete(ds.packDir)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		return lock, nil
	}

	// completed dir exists
	lock, err = lockLatestFromComplete(ds.packDir)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		return lock, nil
	}

	return nil, nil
} 


// acquires lock if complete dir does not exist 
func lockNoComplete(packDir string) (*flock.Flock, error) {

	stat, err := os.Stat(filepath.Join(packDir, dirNameCompleted))
	if err == os.ErrNotExist {
		lock, err := lockActive(packDir, 0)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			return lock, nil
		}
	}
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("active dir is not a dir")
	}

	return nil, nil
}

// packIDstart is the first id it will try to lock
func lockActive(packDir string, packIDstart uint64) (*flock.Flock, error) {
	for i := packIDstart; i < packIDstart + 100; i++ {

		lock := flock.New(filepath.Join(packDir, dirNameActive, packName(i)))

		locked, err := lock.TryLock()
		if err != nil {
			return nil, err
		}

		if locked {
			return lock, nil
		}

		if err := lock.Close(); err != nil {
			return nil, err
		}
	}

	return nil, ErrLimitMaxActive
}

// finds the latest pack in completed dir, and tries to lock any id starting from that
func lockLatestFromComplete(packDir string) (*flock.Flock, error) {
	var latestDir string
	if err := filepath.WalkDir(filepath.Join(packDir, dirNameCompleted), func(path string, d fs.DirEntry, err error) error {
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

	latestPackID, err := packIdFromPath(latestPack)
	if err != nil {
		return nil, err
	}

	nextPackID := latestPackID + 1

	return lockActive(packDir, nextPackID)
}

type fileItem struct {
	name string
	size int64
}

// if flock and err are nil, it means that no active pack was unlocked
func lockLargestPack(dir string) (*flock.Flock, error) {

	files, err := sortedFilesLargestDesc(dir)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, nil
	}

	var locked bool
	var lock *flock.Flock
	for _, f := range files {
		fp := filepath.Join(dirNameActive, f.name)

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
