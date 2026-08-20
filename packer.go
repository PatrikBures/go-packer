package packer

import (
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
