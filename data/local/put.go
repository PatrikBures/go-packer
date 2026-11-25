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

	ds.writePackMu.Lock()

	totalSize := int64(len(data) + prefixLen)

	if ds.currentPackOffset+totalSize >= ds.maxPackSize {
		ds.writePackMu.Unlock()
		return packer.ChunkInfo{}, packer.ErrNewPack
	}


	f := ds.currentPackFile
	offset := ds.currentPackOffset
	packID := ds.currentPackID

	ds.packWrite.Add(packID, f)

	ds.currentPackOffset += totalSize

	ds.writePackMu.Unlock()

	ci, err := putChunk(packID, data, f, offset)
	if err != nil {
		return packer.ChunkInfo{}, err
	}

	ds.writePackMu.Lock()
	defer ds.writePackMu.Unlock()

	if err := ds.packWrite.Dec(packID, ds.currentPackID); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("decrementing packWrite counter: %w", err)
	}

	return ci, nil
}

// Writes data starting at offset to file.
// Prepends the prefix and its data before the chunk.
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
		Offset: uint64(offset),
		Size:   sizeU32,
	}, nil
}
