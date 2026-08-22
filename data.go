package packer

type DataStore interface {
	// will only put the amount in SizeBytes from data
	//
	// returns the offset excluding the headersize but not the size prefix for each chunk
	Put(data []byte) (ChunkInfo, error)
	Get(ChunkInfo, []byte) ([]byte, error)
	NormalizePacks(packs map[uint64]PackInfo)
	// also normalizes
	FilterCompactable(packs map[uint64]PackInfo)
	CompactPacks(packs map[uint64][]ChunkInfo) error
	Close() error
}
