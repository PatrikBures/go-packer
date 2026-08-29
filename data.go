package packer

import (
	"fmt"

	"go.pabu.dev/bytesize"
)

type DataStore interface {
	// will only put the amount in SizeBytes from data
	//
	// returns the offset excluding the headersize but not the size prefix for each chunk
	Put(data []byte) (ChunkInfo, error)
	Get(ChunkInfo, []byte) ([]byte, error)

	// Normalizes each pack size.
	NormalizePacks(packs map[uint64]PackInfo)

	// Removes packs that should not be compacted. It also normalizes each pack.
	FilterCompactable(packs map[uint64]PackInfo) error

	// the provided slice will be split into two slices, the second one
	// returned is a slice containing the chunks for a new pack.
	// The first slice excludes the chunks from the second slice.
	//
	// It is intended to be ran in a loop, each time providing the updated slice.
	CompactNextPack(packs []ChunkInfoWithID) ([]ChunkInfoWithID, []ChunkInfoWithID, CompactInfo, error)

	// Deletes packs, returning total byte amount removed.
	DeletePacks(packs []uint64) (int64, error)

	PacksBatch(startPackID uint64, packs []uint64) ([]uint64, error)

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
	c.PackBytesAdded += other.PackBytesAdded
	c.PacksRemoved += other.PacksRemoved
	c.PacksCreated += other.PacksCreated
	c.ChunksMoved += other.ChunksMoved
}
