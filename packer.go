package packer

import (
	"fmt"
)


type packager struct {
	metadataStore MetadataStore
	dataStore DataStore
}

func NewPackager(ms MetadataStore, ds DataStore, packDir string) *packager {
	return &packager{
		metadataStore: ms,
		dataStore: ds,
	}
}

func (p *packager) Put(id []byte, ci ChunkInfo, data []byte) error {
	if err := p.dataStore.WriteChunk(data); err != nil {
		return fmt.Errorf("failed writing chunk to pack: %w", err)
	}

	if err := p.metadataStore.Put(id, ci); err != nil {
		return fmt.Errorf("failed adding chunk to metadata store: %w", err)
	}

	return nil
}
