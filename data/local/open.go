package local

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/gofrs/flock"
)


func (ds *dataStore) openPackWrite() error {
	if err := os.Mkdir(ds.activeDir(), 0775); err != nil && errors.Is(err, os.ErrExist) {
		return fmt.Errorf("creating active dir: %w", err)
	}

	var lock *flock.Flock
	for range 5 {
		var err error
		time.Sleep(time.Millisecond * time.Duration(rand.Int64N(50)))

		lock, err = ds.tryGetLockOfPack()
		if err != nil {
			return err
		}
		if lock != nil {
			break
		}
	}

	if lock == nil {
		return fmt.Errorf("somehow did not manage to create/get a lock of a pack")
	}

	return nil
}

