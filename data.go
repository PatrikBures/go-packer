package packer

type DataStore interface {
	WriteChunk(data []byte) error
}

