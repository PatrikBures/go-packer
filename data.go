package packer

type DataStore interface {
	// will only put the amount in SizeBytes from data
	//
	// returns the offset excluding the headersize but not the size prefix for each chunk
	Put(data []byte) (ChunkInfo, error)
	Get(ci ChunkInfo) ([]byte, error)
}
