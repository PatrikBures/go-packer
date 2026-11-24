package data

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"testing"

	"go.pabu.dev/packer"
)


func TestCompactToPackLock(t *testing.T) {
	ds := newDS(t, WithMaxPackSize(1024<<20))

	var b [32]byte
	binary.LittleEndian.PutUint64(b[:], 100)
	rng := rand.NewChaCha8(b)

	buf := make([]byte, 128 << 10)

	const count = 1000
	chunks := make([]packer.ChunkInfoWithID, 0, count)

	for range count {
		if _, err := rng.Read(buf); err != nil {
			t.Fatal(err)
		}

		ci, err := ds.Put(buf)
		if err != nil {
			t.Fatal(err)
		}
		id := sha256.Sum256(buf)

		chunks = append(chunks, packer.ChunkInfoWithID{
			ChunkInfo: ci,
			Id: id[:],
		})
	}

	const compactPackID = 1000

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		packFilePath := ds.activePackPath(compactPackID)
		packLockPath := ds.activePackLockPath(compactPackID)
		for i := 0; ; i++ {
			packFileExists := false
			packLockExists := false
			if _, err := os.Stat(packFilePath); !errors.Is(err, os.ErrNotExist) && err != nil {
				t.Fatal(err)
			} else if err == nil {
				packFileExists = true
			}

			if _, err := os.Stat(packLockPath); !errors.Is(err, os.ErrNotExist) && err != nil {
				t.Fatal(err)
			} else if err == nil {
				packLockExists = true
			}

			if packFileExists && !packLockExists {
				t.Fatal("pack exists without lock!")
			}

			if ctx.Err() != nil {
				t.Logf("checked lock %d times", i)
				break
			}
		}
	}()

	ci, err := ds.CompactToPack(compactPackID, chunks)
	if err != nil {
		t.Fatal(err)
	}
	
	t.Log("compact info:", ci.String())

	cancel()
}
