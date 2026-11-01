package packer

import (
	"fmt"

	"go.pabu.dev/bytesize"
)

type DataStore interface {

	// can return ErrNewPack
	Init() error

	CurrentPackID() uint64

	MaxPackSize() int64

	// will only put the amount in Size from ChunkInfo
	//
	// returns the offset excluding the headersize but not the size prefix for each chunk
	Put(data []byte) (ChunkInfo, error)
	Get(ChunkInfo, []byte) ([]byte, error)

	// Normalizes pack size.
	NormalizePackSize(size uint64, chunkCount uint32) uint64

	// Removes packs that should not be compacted. It also normalizes each pack.
	FilterCompactable(packs map[uint64]PackInfo) error

	// the provided slice will be split into two slices, the second one
	// is a slice containing the chunks for a new pack.
	// The first slice excludes the chunks from the second slice.
	//
	// It is intended to be ran in a loop, each time providing the first slice.
	//
	// If the second slice is nil, it means there are not enough chunks to make a full pack.
	// And the loop should break.
	//
	// The provided slice is expected to be sorted by packID and bytes.
	// Meaning that the chunks in smallest packID should be first.
	FillNewPack(chunks []ChunkInfoWithID) (chunksRest []ChunkInfoWithID, chunksNewPack []ChunkInfoWithID)

	// Creates a new pack and puts all the chunks into it.
	// The chunks starts as active and once all chunks have been copied to it, it moves it to complete.
	CompactToPack(packID uint64, chunks []ChunkInfoWithID) (CompactInfo, error)

	// Deletes packs, returning total byte amount removed.
	// Only use for packs that are not referenced by an existing chunk.
	DeletePacks(packs []uint64) (int64, error)

	// Switches current active pack, initializing it. Moves previous pack to completed.
	SwitchPack(packID uint64) error

	// Switches current active pack to a non-locked existing pack. Moves previous pack to completed. 
	SwitchPackExisting() error

	Close() error
}

type CompactInfo struct {
	PackBytesRemoved int64
	PackBytesAdded   int64
	PacksRemoved     int
	PacksCreated     int
	ChunksMoved      int
}

func (ci CompactInfo) String() string {
	return fmt.Sprintf("SizeDiff: % 5s, removed %d and created %d packs, moved %d chunks", 
		bytesize.FormatIEC(ci.PackBytesAdded - ci.PackBytesRemoved),
		ci.PacksRemoved, ci.PacksCreated,
		ci.ChunksMoved,
	)
}

func (c *CompactInfo) Merge(other CompactInfo) {
	c.PackBytesRemoved += other.PackBytesRemoved
	c.PackBytesAdded   += other.PackBytesAdded
	c.PacksRemoved     += other.PacksRemoved
	c.PacksCreated     += other.PacksCreated
	c.ChunksMoved      += other.ChunksMoved
}
