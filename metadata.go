package packer

import (
	"encoding/binary"
)

type MetadataStore interface {

	Init() error

	Close() error

	// Puts or updates a chunk.
	Put(id []byte, ci ChunkInfo) error

	// Puts or updates multiple chunks.
	PutChunks(chunks []ChunkInfoWithID) error

	Get(id []byte) (ChunkInfo, error)

	// Get chunks that are in any of the provided packIDs.
	Chunks(packIDs []uint64) ([]ChunkInfo, error)

	// Get all chunks.
	ChunksAll() ([]ChunkInfo, error)

	// Get all chunk IDs
	ChunkIDsAll() ([][]byte, error)

	// Check if a chunk with id exists.
	Exists(id []byte) (bool, error)

	// Deletes chunks.
	Delete(id [][]byte) error

	PackInfo(packIDs []uint64) (map[uint64]PackInfo, error)

	PackInfoAll() (map[uint64]PackInfo, error)

	ChunksForPacks(packs map[uint64]PackInfo) ([]ChunkInfoWithID, error)

	// Removes packIDs which do not exist, returning the updated slice.
	FilterExistingPacks(packs []uint64) ([]uint64, error)	

	// Returns a batch of packIDs marked for deletion.
	// A pack should be deleted if there are no chunks in it.
	// Will remove the packs returned from the list of packs to be deleted!
	//
	// It is intended to be used for looping through packs to be deleted,
	// and then deleting them in the loop until it returns an empty slice.
	PacksDeleteBatch() ([]uint64, error)

	// Returns all packIDs marked for deletion.
	// Sorted by id, smallest first.
	PacksDelete() ([]uint64, error)

	// Allocates a new unique packID.
	AllocatePack() (uint64, error)

	// Mark pack as completed. If pack does not exist returns nil.
	// If pack has no chunks, marks it for deletion.
	MarkCompleted(packID uint64) error
}




type PackState uint8
const (
	PackStateActive PackState = 0
	PackStateCompleted        = 1
)

const (
	PI1Size      = 0
	PI2Count     = 8
	PI3State     = 12

	PackInfoSize = 13
)

type PackInfo struct {
	Size       uint64    // [0:8]
	ChunkCount uint32    // [8:12]
	State      PackState // [12:13]
}

type PackInfoWithID struct {
	PackInfo
	Id uint64
}

func (pi *PackInfo) Bytes() []byte {
	var buf [PackInfoSize]byte
	pi.PutBytes(buf[:])
	return buf[:]
}

func (pi *PackInfo) PutBytes(buf []byte) {
	_ = buf[PackInfoSize-1] // early bound check
	binary.LittleEndian.PutUint64(buf[PI1Size:PI2Count], pi.Size)
	binary.LittleEndian.PutUint32(buf[PI2Count:PI3State], pi.ChunkCount)
	buf[PI3State] = byte(pi.State)
}

func BytesToPackInfo(b []byte) (PackInfo, error) {
	if len(b) < PackInfoSize {
		return PackInfo{}, ErrSliceTooSmall
	}
	return PackInfo{
		Size:       binary.LittleEndian.Uint64(b[PI1Size:PI2Count]),
		ChunkCount: binary.LittleEndian.Uint32(b[PI2Count:PI3State]),
		State:      PackState(b[PI3State]), // Unsafe conversion, it will crash if the byte is not a valid PackState.
	}, nil
}




type ChunkInfo struct {
	PackID    uint64 // [0:8]
	Offset    uint64 // [8:16]
	Size      uint32 // [16:20]
}

type ChunkInfoWithID struct {
	ChunkInfo
	Id []byte
}

const (
	CI1PackID     = 0
	CI2Offset     = 8
	CI3Size       = 16

	ChunkInfoSize = 20
)

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
	binary.LittleEndian.PutUint64(buf[CI1PackID:CI2Offset], ci.PackID)
	binary.LittleEndian.PutUint64(buf[CI2Offset:CI3Size],   ci.Offset)
	binary.LittleEndian.PutUint32(buf[CI3Size:ChunkInfoSize],        ci.Size)
}

// can return ErrSliceTooSmall
func BytesToChunkInfo(b []byte) (ChunkInfo, error) {
	if len(b) < ChunkInfoSize {
		return ChunkInfo{}, ErrSliceTooSmall
	}
	return ChunkInfo{
		PackID:    binary.LittleEndian.Uint64(b[CI1PackID:CI2Offset]),
		Offset:    binary.LittleEndian.Uint64(b[CI2Offset:CI3Size]),
		Size:      binary.LittleEndian.Uint32(b[CI3Size:ChunkInfoSize]),
	}, nil
}
