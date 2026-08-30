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
	defer p.pool.Put(&buf)

	data, err := p.dataStore.Get(ci, buf)
	if err != nil {
		return nil, fmt.Errorf("getting data from dataStore: %w", err)
	}

	return data, nil
}

func (p *Packager) Delete(ids [][]byte) error {
	return p.metadataStore.Delete(ids)
}

func (p *Packager) Chunks(packIDs []uint64) ([]ChunkInfo, error) {
	return p.metadataStore.Chunks(packIDs)
}

func (p *Packager) ChunkIDsAll() ([][]byte, error) {
	return p.metadataStore.ChunkIDsAll()
}

func (p *Packager) PackInfo(packIDs []uint64) (map[uint64]PackInfo, error) {
	packs, err := p.metadataStore.PackInfo(packIDs)
	if err != nil {
		return nil, err
	}
	p.dataStore.NormalizePacks(packs)
	return packs, nil
}
func (p *Packager) PackInfoAll() (map[uint64]PackInfo, error) {
	packs, err := p.metadataStore.PackInfoAll()
	if err != nil {
		return nil, err
	}
	p.dataStore.NormalizePacks(packs)
	return packs, nil
}


// Gets all packs with their chunks to be relocated to new packs.
// Should be passed to CompactNext in a loop
func (p *Packager) CompactablePackChunks() ([]ChunkInfoWithID, error) {
	packs, err := p.metadataStore.PackInfoAll()
	if err != nil {
		return nil, fmt.Errorf("getting all packs: %w", err)
	}
	if err := p.dataStore.FilterCompactable(packs); err != nil {
		return nil, err
	}

	if len(packs) == 0 {
		return nil, nil
	}

	chunks, err := p.metadataStore.ChunksForPacks(packs)
	if err != nil {
		return nil, fmt.Errorf("getting chunks for each pack: %w", err)
	}
	return chunks, nil
}

// The returned slice should be passed again to this method in a loop.
// It should break out of the loop when CompactInfo.PacksCreated = 0.
func (p *Packager) CompactNext(chunks []ChunkInfoWithID) ([]ChunkInfoWithID, CompactInfo, error) {
	newChunks, packChunks, compInfo, err := p.dataStore.CompactNextPack(chunks)
	if err != nil {
		return nil, CompactInfo{}, fmt.Errorf("compacting next pack: %w", err)
	}
	if err := p.metadataStore.UpdateChunks(packChunks); err != nil {
		return nil, CompactInfo{}, fmt.Errorf("updating %d chunks: %w", len(chunks), err)
	}

	return newChunks, compInfo, nil
}

func (p *Packager) Compact() (CompactInfo, error) {
	chunks, err := p.CompactablePackChunks()
	if err != nil {
		return CompactInfo{}, err
	}

	var compInfoAll CompactInfo

	for {
		newChunks, compInfo, err := p.CompactNext(chunks)
		if err != nil {
			return CompactInfo{}, err
		}
		chunks = newChunks
		if compInfo.PacksCreated == 0 {
			break
		}
		compInfoAll.Merge(compInfo)
	}
	duInfo, err := p.DeleteUnreferencedPacks()
	if err != nil {
		return compInfoAll, err
	}
	compInfoAll.Merge(duInfo)

	return compInfoAll, nil
}

// Returns the packs to be compacted
func (p *Packager) WouldCompact() (map[uint64]PackInfo, error) {
	packs, err := p.metadataStore.PackInfoAll()
	if err != nil {
		return nil, fmt.Errorf("getting all packs: %w", err)
	}
	if err := p.dataStore.FilterCompactable(packs); err != nil {
		return nil, err
	}
	return packs, nil
}

func (p *Packager) DeleteUnreferencedPacks() (info CompactInfo, err error) {
	var nextPackID uint64
	var packs []uint64

	for {
		if len(packs) > 0 {
			packs = packs[:0]
		}
		packs, err = p.dataStore.PacksBatch(nextPackID, packs)
		if err != nil {
			return CompactInfo{}, err
		}
		if len(packs) == 0 || nextPackID-1 == packs[len(packs)-1] {
			break
		}
		nextPackID = packs[len(packs)-1] + 1

		packs, err = p.metadataStore.FilterExistingPacks(packs)
		if err != nil {
			return CompactInfo{}, err
		}

		bytesRemoved, err := p.dataStore.DeletePacks(packs)
		if err != nil {
			return CompactInfo{}, err
		}

		info.PacksRemoved += len(packs)
		info.PackBytesRemoved += bytesRemoved
	}
	return
}
