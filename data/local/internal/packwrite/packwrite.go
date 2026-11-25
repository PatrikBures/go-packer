package packwrite

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// it is not concurrently safe, so ensure that only one routine is modifying it.
type PackWrite struct {
	packs map[uint64]pack
}

type pack struct {
	f *os.File
	writers uint64
	switched time.Time
}

func New() PackWrite {
	return PackWrite{
		packs: make(map[uint64]pack),
	}
}

// Adds 1 writer to pack. Creates a new entry using the provided
// file, if an entry for the packID does not already exist.
func (pw *PackWrite) Add(packID uint64, f *os.File) {
	if p, exists := pw.packs[packID]; exists {
		p.writers++
		pw.packs[packID] = p
	} else {
		pw.packs[packID] = pack{
			f: f,
			writers: 1,
			switched: time.Now(),
		}
	}
}

// Decrements the amount of writers for pack, if it becomes zero
// and packID is not currentPackID, it will close it and return its
// error if there is any.
func (pw *PackWrite) Dec(packID uint64, currentPackID uint64) error {
	p, exists := pw.packs[packID]

	if !exists {
		return fmt.Errorf("pack with packID %d not found", packID)
	}

	if p.writers == 0 {
		return fmt.Errorf("writers is 0 before decreasing it, it can not be decreased more")
	}

	p.writers--

	if p.writers == 0 {
		if currentPackID != packID {
			if err := close(p.f); err != nil {
				return fmt.Errorf("closing pack file with packID %d: %w", packID, err)
			}
		}
		delete(pw.packs, packID)
	}

	return nil
}

func (pw *PackWrite) CloseSwitchedLaterThan(dur time.Duration) (err error) {
	now := time.Now()

	var closes int
	var errs int
	for k, v := range pw.packs {
		diff := now.Sub(v.switched)
		if diff >= dur {
			ec := close(v.f)
			if ec != nil {
				err = errors.Join(err, fmt.Errorf("closing pack file with id %d: %w", k, ec))
				errs++
			}
			closes++
		}
	}

	if err != nil {
		return fmt.Errorf("%d/%d write packs closing errored: %w", errs, closes, err)
	}

	return nil
}

func close(f *os.File) error {
	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}
	return nil
}
