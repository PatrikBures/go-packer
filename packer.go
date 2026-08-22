package packer

import (
	"errors"
	"fmt"
	"sync"
)

type Packager struct {
	metadataStore MetadataStore
	dataStore     DataStore
	pool          sync.Pool
}

func New(ms MetadataStore, ds DataStore) *Packager {
	return &Packager{
		metadataStore: ms,
		dataStore:     ds,
		pool: sync.Pool{
			New: func() any {
				return make([]byte, 1024<<10)
			},
		},
	}
}

func (p *Packager) Close() error {
	return errors.Join(p.metadataStore.Close(), p.dataStore.Close())
}

func (p *Packager) Put(id []byte, data []byte) error {
	if ok, err := p.metadataStore.Exists(id); err != nil {
		return fmt.Errorf("checking if chunk already exists: %w", err)
	} else if ok {
		return nil
	}

	ci, err := p.dataStore.Put(data)
	if err != nil {
		return fmt.Errorf("failed writing chunk to pack: %w", err)
	}

	if err := p.metadataStore.Put(id, ci); err != nil {
		return fmt.Errorf("failed adding chunk to metadata store: %w", err)
	}

	return nil
}

func (p *Packager) Get(id []byte) ([]byte, error) {
	ci, err := p.metadataStore.Get(id)
	if err != nil {
		return nil, fmt.Errorf("getting id from metadataStore: %w", err)
	}

	buf := p.pool.Get().([]byte)
	defer p.pool.Put(buf)

	data, err := p.dataStore.Get(ci, buf)
	if err != nil {
		return nil, fmt.Errorf("getting data from dataStore: %w", err)
	}

	return data, nil
}

func (p *Packager) Delete(id []byte) error {
	return p.metadataStore.Delete(id)
}

func (p *Packager) Chunks(packIDs []uint64) ([]ChunkInfo, error) {
	return p.metadataStore.Chunks(packIDs)
}

func (p *Packager) PackInfo(packIDs []uint64) (map[uint64]PackInfo, error) {
	packs, err := p.metadataStore.PackInfo(packIDs)
	if err != nil { return nil, err }
	p.dataStore.NormalizePacks(packs)
	return packs, nil
}
func (p *Packager) PackInfoAll() (map[uint64]PackInfo, error) {
	packs, err := p.metadataStore.PackInfoAll()
	if err != nil { return nil, err }
	p.dataStore.NormalizePacks(packs)
	return packs, nil
}

func (p *Packager) Compact() error {
	packs, err := p.metadataStore.PackInfoAll()
	if err != nil {
		return fmt.Errorf("getting all packs: %w", err)
	}
	p.dataStore.FilterCompactable(packs)

	if len(packs) == 0 {
		return nil
	}

	_, err = p.metadataStore.ChunksForPacks(packs)
	if err != nil {
		return fmt.Errorf("getting chunks for each pack: %w", err)
	}


	return nil
}

// Returns the packs that would be compacted by Compact()
func (p *Packager) WouldCompact() (map[uint64]PackInfo, error) {
	packs, err := p.metadataStore.PackInfoAll()
	if err != nil {
		return nil, fmt.Errorf("getting all packs: %w", err)
	}
	p.dataStore.FilterCompactable(packs)
	return packs, nil
}
