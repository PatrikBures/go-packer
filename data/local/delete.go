package data

import (
	"errors"
	"fmt"
	"os"
)

// Ignores deletion of any active packs.
// Removes pack from read cache.
func (ds *dataStore) DeletePacks(packs []uint64) (bytesRemoved int64, err error) {
	for _, packID := range packs {

		var stat os.FileInfo

		// stats file, continuing if it exists in active
		stat, err = os.Stat(ds.completedPackPath(packID))
		if errors.Is(err, os.ErrNotExist) {
			// this still causes the next err check to error if active does not exist
			if _, aerr := os.Stat(ds.activePackPath(packID)); aerr == nil {
				continue
			}
		}
		if err != nil {
			return bytesRemoved, fmt.Errorf("stating pack: %w", err)
		}

		_ = ds.packRead.ClosePack(packID)

		size := stat.Size()

		err = os.Remove(ds.completedPackPath(packID))
		if errors.Is(err, os.ErrNotExist) {
			if _, aerr := os.Stat(ds.activePackPath(packID)); aerr == nil {
				continue
			}
		}	
		if err != nil {
			return bytesRemoved, fmt.Errorf("removing file pack: %w", err)
		}

		bytesRemoved += size
	}
	return bytesRemoved, nil
}
