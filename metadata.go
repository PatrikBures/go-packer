package packer

import (
	"encoding/binary"
	"errors"
)

type MetadataStore interface {

	Close() error

	// Puts or updates a chunk.
	Put(id []byte, ci ChunkInfo) error

	// Puts or updates multiple chunks.
	PutChunks(chunks []ChunkInfoWithID) error

	Get(id []byte) (ChunkInfo, error)

	// Check if a chunk with id exists.
	Exists(id []byte) (bool, error)

	Delete(id [][]byte) error

	// Get packs that are in any of the provided packIDs.
	Chunks(packIDs []uint64) ([]ChunkInfo, error)

	// Get all chunk IDs
	ChunkIDsAll() ([][]byte, error)

	PackInfo(packIDs []uint64) (map[uint64]PackInfo, error)

	PackInfoAll() (map[uint64]PackInfo, error)

	ChunksForPacks(packs map[uint64]PackInfo) ([]ChunkInfoWithID, error)

	// Removes packIDs which do not exist, returning the updated slice.
	FilterExistingPacks(packs []uint64) ([]uint64, error)	

	// Returns a batch of packIDs that should be deleted.
	// A pack should be deleted if there are no chunks in it.
	PacksDeleteBatch() ([]uint64, error)
}

var (
	ErrSliceTooSmall = errors.New("provided slice is too small")
)



type PackState uint8
const (
	PackStateActive PackState = 0
	PackStateCompleted        = 1
)

const PackInfoSize = 13
type PackInfo struct {
	ChunkTotalSize uint64    // [0:8]
	ChunkCount     uint32    // [8:12]
	State          PackState // [12:13]
}

func (pi *PackInfo) Bytes() []byte {
	var buf [PackInfoSize]byte
	pi.PutBytes(buf[:])
	return buf[:]
}

func (pi *PackInfo) PutBytes(buf []byte) {
	_ = buf[PackInfoSize-1] // early bound check
	binary.LittleEndian.PutUint64(buf[0:8], pi.ChunkTotalSize)
	binary.LittleEndian.PutUint32(buf[8:12], pi.ChunkCount)
	buf[12] = byte(pi.State)
}

func BytesToPackInfo(b []byte) (PackInfo, error) {
	if len(b) < PackInfoSize {
		return PackInfo{}, ErrSliceTooSmall
	}
	return PackInfo{
		ChunkTotalSize: binary.LittleEndian.Uint64(b[0:8]),
		ChunkCount:     binary.LittleEndian.Uint32(b[8:12]),
		State:          PackState(b[12]), // Unsafe conversion, it will crash if the byte is not a valid PackState.
	}, nil
}



const ChunkInfoSize = 20

type ChunkInfo struct {
	PackID    uint64 // [0:8]
	Offset    uint64 // [8:16]
	SizeBytes uint32 // [16:20]
}

type ChunkInfoWithID struct {
	ChunkInfo
	Id []byte
}

func (ci *ChunkInfo) Bytes() []byte {
	var buf [ChunkInfoSize]byte
	ci.PutBytes(buf[:])
	return buf[:]
}

// converts ChunkInfo into buf[0:20]
//
// panics if buf is len is less than 20
func (ci *ChunkInfo) PutBytes(buf []byte) {
	_ = buf[ChunkInfoSize-1] // early bound check
	binary.LittleEndian.PutUint64(buf, ci.PackID)
	binary.LittleEndian.PutUint64(buf[8:16], ci.Offset)
	binary.LittleEndian.PutUint32(buf[16:20], ci.SizeBytes)
}

// can return ErrSliceTooSmall
func BytesToChunkInfo(b []byte) (ChunkInfo, error) {
	if len(b) < ChunkInfoSize {
		return ChunkInfo{}, ErrSliceTooSmall
	}
	return ChunkInfo{
		PackID:    binary.LittleEndian.Uint64(b[0:8]),
		Offset:    binary.LittleEndian.Uint64(b[8:16]),
		SizeBytes: binary.LittleEndian.Uint32(b[16:20]),
	}, nil
}
