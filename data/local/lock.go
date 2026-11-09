package data

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.pabu.dev/packer/data/local/internal/pulser"
)


func (ds *dataStore) tryLockPack(packID uint64) (locked bool, err error) {
	lockPath := ds.activePackLockPath(packID)

	var f *os.File

	for range 2 {
		f, err = os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_TRUNC|os.O_WRONLY, 0644)
		if errors.Is(err, os.ErrExist) {

			stat, err := os.Stat(lockPath)
			if errors.Is(err, os.ErrNotExist) {
				continue
			} else if err != nil {
				return false, fmt.Errorf("stating existing lock file: %w", err)
			}

			if time.Since(stat.ModTime()) < ds.lockStaleDuration {
				return false, nil
			}
			// if pack is older than lockStaleDuration, unlock the pack, and try to lock again
			if err = ds.unlockPack(packID); errors.Is(err, os.ErrNotExist) {
				continue
			} else if err != nil {
				return false, fmt.Errorf("unlocking old lock: %w", err)
			}

		} else if err != nil {
			return false, fmt.Errorf("opening lock file: %w", err)
		}

		break
	}

	// did not manage to get lock
	if f == nil {
		return false, nil
	}

	defer func() {
		if err == nil { return }
		if uerr := ds.unlockPack(packID); uerr != nil {
			err = errors.Join(err, fmt.Errorf("unlocking pack %d after error: %w", packID, uerr))
		}
	}()

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "N/A"
	}

	if _, err = f.WriteString(fmt.Sprintf("host:%q, pid:%d, started:%q\n", hostname, os.Getpid(), time.Now().Format(time.RFC3339))); err != nil {
		return false, fmt.Errorf("writing info to lock file: %w", err)
	}

	if err = f.Close(); err != nil {
		return false, fmt.Errorf("closing lock file after successfull lock: %w", err)
	}

	return true, nil
}

func (ds *dataStore) unlockPack(packID uint64) error {
	return os.Remove(ds.activePackLockPath(packID))
}

func (ds *dataStore) startPackPulse(packID uint64) *pulser.Pulse {
	return pulser.Start(ds.activePackLockPath(packID), ds.lockPulseInterval)
}

type fileItem struct {
	name string
	size int64
}

// if the bool and error are nil, it means that no active pack was locked.
func (ds *dataStore) lockLargestPack(minSize int64) (bool, uint64, error) {

	files, err := sortedFilesLargestDesc(ds.activeDir())
	if err != nil {
		return false, 0, fmt.Errorf("getting files sorted by size: %w", err)
	}

	if len(files) == 0 {
		return false, 0, nil
	}

	var locked bool
	var packID uint64
	for _, f := range files {
		packID, err = packIdFromPath(f.name)
		if err != nil {
			return false, 0, err
		}

		// skips pack that is currently open
		if ds.currentPackFile != nil && packID == ds.currentPackID {
			continue
		}

		// skips pack that does not have enough room for a chunk of minSize
		if f.size+minSize+prefixLen > ds.maxPackSize {
			continue
		}

		locked, err = ds.tryLockPack(packID)
		if err != nil {
			return false, 0, fmt.Errorf("locking file: %w", err)
		}

		if !locked {
			continue
		}

		// Double checking that there is room for chunk. The size 
		// could have changed between the first check and before locking.
		stat, err := os.Stat(ds.activePackPath(packID))
		if errors.Is(err, os.ErrNotExist) {
			if err = ds.unlockPack(packID); err != nil {
				return false, 0, fmt.Errorf("unlocking after finding out pack file does not exist: %w", err)
			}
			continue
		} else if err != nil {
			if uerr := ds.unlockPack(packID); err != nil {
				err = errors.Join(err, fmt.Errorf("unlocking pack: %w", uerr))
			}
			return false, 0, fmt.Errorf("stating pack after locking: %w", err)
		}
		if stat.Size()+minSize+prefixLen > ds.maxPackSize {
			if err = ds.unlockPack(packID); err != nil {
				return false, 0, fmt.Errorf("unlocking after pack not enough room: %w", err)
			}
			continue
		}
	}

	if !locked {
		return false, 0, nil
	}

	return true, packID, nil
}

func sortedFilesLargestDesc(dir string) ([]fileItem, error) {
	var files []fileItem

	locks := make(map[string]struct{})

	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}

		if strings.HasSuffix(info.Name(), ".lock") {
			locks[strings.TrimSuffix(info.Name(), ".lock")] = struct{}{}
			return nil
		}

		files = append(files, fileItem{
			name: info.Name(),
			size: info.Size(),
		})

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking dir: %w", err)
	}


	// filters out locked files
	n := 0
	for _, f := range files {
		if _, ok := locks[f.name]; ok {
			files[n] = f
			n++
		}
	}
	files = files[:n]

	sort.Slice(files, func(i, j int) bool {
		return files[i].size > files[j].size
	})

	return files, nil
}
