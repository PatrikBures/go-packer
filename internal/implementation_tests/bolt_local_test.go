package imptests

import (
	"encoding/binary"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"go.pabu.dev/packer"
	data "go.pabu.dev/packer/data/local"
	metadata "go.pabu.dev/packer/metadata/bolt"
	"lukechampine.com/blake3"
)

func FuzzBoltLocal(f *testing.F) {
	fuzzAdd(f)
	f.Fuzz(fuzzPackagerBoltLocal)
}

func fuzzAdd(f *testing.F) {
	// seed, maxPackSize (divides by 32), maxChunkSize (divides by 256)
	f.Add(uint64(12345), uint32(64<<20), uint32(64<<20))
}

func fuzzPackagerBoltLocal(t *testing.T, seed uint64, maxPackSize uint32, maxChunkSize uint32) {
	maxPackSize /= 128
	maxChunkSize /= 1024

	td := t.TempDir()

	ms, err := metadata.Init(filepath.Join(td, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}

	ds, err := data.Init(td, data.WithMaxPackSize(int64(maxPackSize)))
	if err != nil {
		t.Fatal(err)
	}

	p := packer.New(ms, ds)

	var seedBytes [32]byte
	binary.LittleEndian.PutUint64(seedBytes[:8], seed)
	rng := rand.NewChaCha8(seedBytes)
	
	mainFuzzLoop(t, p, rng, maxChunkSize)
}

func mainFuzzLoop(t *testing.T, p *packer.Packager, rng *rand.ChaCha8, maxChunkSize uint32) {
	rngr := rand.New(rng)

	buf := make([]byte, maxChunkSize)

	for {
		action := rngr.Uint32N(100)
		if action <= 10 {
			chunkIDs, err := p.ChunkIDsAll()
			if err != nil {
				t.Fatalf("getting all chunk IDs: %v", err)
			}
			if err := p.Delete(chunkIDs[:len(chunkIDs)/4]); err != nil {
				t.Fatalf("deleting chunks: %v", err)
			}
		} else if action <= 15 {
			_, err := p.Compact()
			if err != nil {
				t.Fatalf("compacting: %v", err)
			}
		} else {
			chunkSize := rngr.Uint32N(maxChunkSize)
			chunkBytes := buf[:chunkSize]

			if _, err := rng.Read(chunkBytes); err != nil {
				t.Fatalf("generating random chunk: %v", err)
			}

			hash := blake3.Sum256(chunkBytes)
			if err := p.Put(hash[:], chunkBytes); err != nil {
				t.Fatalf("putting chunk: %v", err)
			}
		}
	}

}
