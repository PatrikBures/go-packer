package packer

import (
	"errors"
	"fmt"
)

type Packager struct {
	metadataStore MetadataStore
	dataStore     DataStore
}

func New(ms MetadataStore, ds DataStore) *Packager {
	return &Packager{
		metadataStore: ms,
		dataStore:     ds,
	}
}

func (p *Packager) Init() error {
	if err := p.metadataStore.Init(); err != nil {
		return fmt.Errorf("initializing metadataStore: %w", err)
	}

	if err := p.dataStore.Init(); errors.Is(err, ErrNewPack) {
		newPackID, err := p.metadataStore.AllocatePack()
		if err != nil {
			return fmt.Errorf("allocating pack: %w", err)
		}
		if err := p.dataStore.SwitchPack(newPackID); err != nil {
			return fmt.Errorf("switching pack to %d: %w", newPackID, err)
		}
	}

	return nil
}

func (p *Packager) Close() error {
	return errors.Join(p.metadataStore.Close(), p.dataStore.Close())
}

// Puts a pack with a unique id. Returns early if the id already exists in metadataStore. 
func (p *Packager) Put(id []byte, data []byte) error {
	if exists, err := p.metadataStore.Exists(id); err != nil {
		return fmt.Errorf("checking if chunk already exists: %w", err)
	} else if exists {
		return nil
	}

	for range 2 {

		ci, err := p.dataStore.Put(data)

		// when a new pack is needed, like when the current one is full,
		// it first checks if there is an existing one it can continue on,
		// otherwise it allocates a new one.
		if errors.Is(err, ErrNewPack) {
			err := p.dataStore.SwitchPackExisting()
			if errors.Is(err, ErrNewPack) {
				newPackID, err := p.metadataStore.AllocatePack()
				if err != nil {
					return fmt.Errorf("allocating new pack: %w", err)
				}
				if err := p.dataStore.SwitchPack(newPackID); err != nil {
					return fmt.Errorf("switching pack %d: %w", newPackID, err)
				}
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("failed writing chunk to pack: %w", err)
		}
		if err := p.metadataStore.Put(id, ci); err != nil {
			return fmt.Errorf("failed adding chunk to metadata store: %w", err)
		}
		return nil
	}

	return fmt.Errorf("put retries exhausted")
}

// Gets content of chunk.
func (p *Packager) Get(id []byte, buf []byte) ([]byte, error) {
	ci, err := p.metadataStore.Get(id)
	if err != nil {
		return nil, fmt.Errorf("getting id from metadataStore: %w", err)
	}

	data, err := p.dataStore.Get(ci, buf)
	if err != nil {
		return nil, fmt.Errorf("getting data from dataStore: %w", err)
	}

	return data, nil
}

// Deletes multiple chunks
func (p *Packager) Delete(ids [][]byte) error {
	return p.metadataStore.Delete(ids)
}

// Get all chunk metadata in provided packs
func (p *Packager) Chunks(packIDs []uint64) ([]ChunkInfo, error) {
	return p.metadataStore.Chunks(packIDs)
}

// Get all chunk ids
func (p *Packager) ChunkIDsAll() ([][]byte, error) {
	return p.metadataStore.ChunkIDsAll()
}

// Get PackInfo for packs
func (p *Packager) PackInfo(packIDs []uint64) (map[uint64]PackInfo, error) {
	packs, err := p.metadataStore.PackInfo(packIDs)
	if err != nil {
		return nil, err
	}
	p.dataStore.NormalizePacks(packs)
	return packs, nil
}

// Get PackInfo for all packs
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
	fmt.Printf("\nCompactable: %d packs from metadata\n", len(packs))
	if err != nil {
		return nil, fmt.Errorf("getting all packs: %w", err)
	}
	if err := p.dataStore.FilterCompactable(packs); err != nil {
		return nil, err
	}
	fmt.Printf("Compactable: %d packs after filter\n", len(packs))

	if len(packs) == 0 {
		return nil, nil
	}

	chunks, err := p.metadataStore.ChunksForPacks(packs)
	if err != nil {
		return nil, fmt.Errorf("getting chunks for each pack: %w", err)
	}
	return chunks, nil
}

// Takes chunks from chunks and if the sizes are enough it creates a new pack with them.
//
// The returned slice should be passed again to this method in a loop.
// It should break out of the loop when CompactInfo.PacksCreated = 0.
func (p *Packager) CompactNext(chunks []ChunkInfoWithID) ([]ChunkInfoWithID, CompactInfo, error) {
	chunksRest, chunksNewPack := p.dataStore.FillNewPack(chunks)
	if chunksNewPack == nil {
		return chunksRest, CompactInfo{}, nil
	}

	newPackID, err := p.metadataStore.AllocatePack()
	if err != nil {
		return nil, CompactInfo{}, fmt.Errorf("allocating new pack: %w", err)
	}

	compInfo, err := p.dataStore.CompactNextPack(newPackID, chunks)
	if err != nil {
		return nil, CompactInfo{}, fmt.Errorf("compacting next pack: %w", err)
	}
	if err := p.metadataStore.PutChunks(chunksNewPack); err != nil {
		return nil, CompactInfo{}, fmt.Errorf("updating %d chunks: %w", len(chunks), err)
	}

	return chunksRest, compInfo, nil
}


// Compacts all chunks using CompactablePackChunks, CompactNext and DeleteUnreferencedPacks
func (p *Packager) Compact() (CompactInfo, error) {
	chunks, err := p.CompactablePackChunks()
	if err != nil {
		return CompactInfo{}, err
	}

    fmt.Printf("Compact: got %d chunks to compact\n", len(chunks))  // ADD

	var compInfoAll CompactInfo
	for i := 0 ; ; i++ {
		newChunks, compInfo, err := p.CompactNext(chunks)
		if err != nil {
			return CompactInfo{}, err
		}
        fmt.Printf("Compact loop %d: newChunks=%d PacksCreated=%d\n", i, len(newChunks), compInfo.PacksCreated)
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

// Deletes packs from dataStore which are not referenced in metadataStore
func (p *Packager) DeleteUnreferencedPacks() (info CompactInfo, err error) {
	var nextPackID uint64
	var packs []uint64

	for {
		if len(packs) > 0 {
			packs = packs[:0]
		}
		packs, err = p.metadataStore.PacksDeleteBatch()
		if err != nil {
			return CompactInfo{}, err
		}
		if len(packs) == 0 || nextPackID-1 == packs[len(packs)-1] {
			break
		}
		nextPackID = packs[len(packs)-1] + 1

		bytesRemoved, err := p.dataStore.DeletePacks(packs)
		if err != nil {
			return CompactInfo{}, err
		}

		info.PacksRemoved += len(packs)
		info.PackBytesRemoved += bytesRemoved
	}
	return
}
