package data

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"

	"github.com/gofrs/flock"
)


type fileItem struct {
	name string
	size int64
}

// if flock and err are nil, it means that no active pack was locked.
func (ds *dataStore) lockLargestPack(minSize int64) (*flock.Flock, uint64, error) {

	files, err := sortedFilesLargestDesc(ds.activeDir())
	if err != nil {
		return nil, 0, err
	}

	if len(files) == 0 {
		return nil, 0, nil
	}

	var locked bool
	var lock *flock.Flock
	var packID uint64
	for _, f := range files {
		packID, err = packIdFromPath(f.name)
		if err != nil {
			return nil, 0, err
		}

		// skips pack that is currently open
		if ds.currentPackFile != nil && packID == ds.currentPackID {
			continue
		}

		// skips pack that does not have enough room for a chunk of minSize
		if f.size+minSize+prefixLen > ds.maxPackSize {
			continue
		}

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
