package packer

type DataStore interface {
	// will only put the amount in SizeBytes from data
	//
	// returns the offset excluding the headersize but not the size prefix for each chunk
	Put(data []byte) (ChunkInfo, error)
	Get(ChunkInfo, []byte) ([]byte, error)
	NormalizePacks(packs map[uint64]PackInfo)
	// also normalizes
	FilterCompactable(packs map[uint64]PackInfo) error
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

func (c *CompactInfo) Merge(other CompactInfo) {
	c.PackBytesRemoved += other.PackBytesRemoved
	c.PackBytesAdded += other.PackBytesAdded
	c.PacksRemoved += other.PacksRemoved
	c.PacksCreated += other.PacksCreated
	c.ChunksMoved += other.ChunksMoved
}
