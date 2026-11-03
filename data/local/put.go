package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"

	"go.pabu.dev/packer"
)

func (ds *dataStore) Put(data []byte) (packer.ChunkInfo, error) {
	if len(data) > math.MaxUint32 {
		return packer.ChunkInfo{}, errors.New("data is larger than MaxUint32")
	}

	// using defer mutex does not allow multiple writes to same pack.
	// in the futurure, it could allocate the required data size, and then write.
	// that should allow multiple writes to same pack.
	//
	// the issue is with having just one chunk allowed to be written to a pack,
	// is that if one user has slow upload speed, it will slow down other users
	// writing to the same pack.
	//
	// ideally it would unlock once it allocated the chunk in the pack.
	// or it could pre-allocate the entire pack with maxPackSize, and then trim it
	// once the pack becomes full and is moved to completed.

	ds.writePackMu.Lock()
	defer ds.writePackMu.Unlock()

	totalSize := int64(len(data) + prefixLen)

	if ds.currentPackOffset+totalSize >= ds.maxPackSize {
		return packer.ChunkInfo{}, packer.ErrNewPack
	}

	ci, err := putChunk(ds.currentPackID, data, ds.currentPackFile, ds.currentPackOffset)
	if err != nil {
		return packer.ChunkInfo{}, err
	}

	ds.currentPackOffset += totalSize

	return ci, nil
}

// Writes data starting at offset to file.
// Prepends the prefix and its data before the chunk.
// Removes headerLen from the final offset (not sure if to keep).
//
// packID is just used for the returning ChunkInfo.
func putChunk(packID uint64, data []byte, f *os.File, offset int64) (packer.ChunkInfo, error) {
	if len(data) > math.MaxUint32 {
		return packer.ChunkInfo{}, errors.New("data is larger than MaxUint32")
	}

	sizeU32 := uint32(len(data))

	var prefixBytes [prefixLen]byte
	binary.LittleEndian.PutUint32(prefixBytes[:], sizeU32)

	// write size prefix
	if _, err := f.WriteAt(prefixBytes[:], offset); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing size: %w", err)
	}
	// write data
	if _, err := f.WriteAt(data, offset+prefixLen); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing data: %w", err)
	}

	if err := f.Sync(); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("syncing chunk: %w", err)
	}

	return packer.ChunkInfo{
		PackID: packID,
		Offset: uint64(offset - headerLen),
		Size:   sizeU32,
	}, nil
}
