package local

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"go.pabu.dev/packer"
)

func (ds *dataStore) NormalizePacks(packs map[uint64]packer.PackInfo) {
	for k, v := range packs {
		packs[k] = normalizePack(v)
	}
}

func (ds *dataStore) FilterCompactable(packs map[uint64]packer.PackInfo) {
	for packID, info := range packs {
		np := normalizePack(info)
		if !ds.shouldCompact(np) {
			delete(packs, packID)
		} else {
			packs[packID] = np
		}
	}
}

func (ds *dataStore) shouldCompact(pack packer.PackInfo) bool {
	return pack.ChunkTotalSize < uint64(ds.compactPackThreshold)
}

func normalizePack(pack packer.PackInfo) packer.PackInfo {
	pack.ChunkTotalSize += headerLen + (pack.ChunkCount * prefixLen)
	return pack
}

func (ds *dataStore) CompactNextPack(packChunks map[uint64][]packer.ChunkInfoWithID) (chunks []packer.ChunkInfoWithID, compactInfo packer.CompactInfo, err error) {

	var newPack []packer.ChunkInfoWithID
	newPack = ds.fillNewPack(packChunks)
	if newPack == nil {
		return
	}

	lock, err := ds.lockActive(ds.currentPackID + 1)
	if err != nil {
		err = fmt.Errorf("locking file: %w", err)
		return
	}
	defer func() {
		if !lock.Locked() {
			return
		}
		if err != nil {
			_ = lock.Close()
			return
		}
		err = lock.Unlock()
		if err != nil {
			err = fmt.Errorf("unlocking pack: %w", err)
		}
	}()

	packID, err := packIdFromPath(lock.Path())
	if err != nil {
		return
	}

	f, err := os.Create(lock.Path())
	if err != nil {
		err = fmt.Errorf("creating file: %w", err)
		return
	}
	defer func() {
		if f != nil && err != nil {
			_ = f.Close()
			_ = os.Remove(lock.Path())
		}
	}()

	h, err := writePackHeader(packID, stateCompacting, f)
	if err != nil {
		err = fmt.Errorf("writing header to new pack: %w", err)
		return
	}

	offset := int64(headerLen)
	var buf []byte // should move to pool in future
	for _, ci := range newPack {
		totalSize := ci.SizeBytes + prefixLen
		if len(buf) < int(totalSize) {
			buf = slices.Grow(buf, int(totalSize)-len(buf))[:totalSize:totalSize]
		}

		data, err := ds.Get(ci.ChunkInfo, buf)
		if err != nil {
			return nil, packer.CompactInfo{}, fmt.Errorf("getting chunk: %w", err)
		}

		newCi, err := ds.put(packID, data, f, offset)
		if err != nil {
			return nil, packer.CompactInfo{}, fmt.Errorf("puting chunk into new pack: %w", err)
		}

		offset += int64(totalSize)
		ci.ChunkInfo = newCi
		compactInfo.ChunksMoved++
		chunks = append(chunks, ci)
	}

	compactInfo.PackBytesAdded = offset
	compactInfo.PacksCreated = 1

	// ensure header is written to start of file
	if _, err = f.Seek(0, 0); err != nil {
		return nil, packer.CompactInfo{}, fmt.Errorf("seeking to start of pack: %w", err)
	}

	h.State = stateCompleted
	if _, err = h.WriteTo(f); err != nil {
		return nil, packer.CompactInfo{}, fmt.Errorf("writing updated header: %w", err)
	}

	newPath := ds.completedPackPath(packID)
	if err := os.MkdirAll(filepath.Dir(newPath), 0775); err != nil {
		return nil, packer.CompactInfo{}, err
	}

	if err = f.Sync(); err != nil {
		return nil, packer.CompactInfo{}, fmt.Errorf("syncing file pack: %w", err)
	}

	if err = f.Close(); err != nil {
		return nil, packer.CompactInfo{}, fmt.Errorf("closing pack file: %w", err)
	}

	if err = lock.Unlock(); err != nil {
		return nil, packer.CompactInfo{}, fmt.Errorf("unlocking pack file: %w", err)
	}

	if err := os.Rename(lock.Path(), newPath); err != nil {
		_ = os.Remove(lock.Path())
		return nil, packer.CompactInfo{}, fmt.Errorf("renaming compacted pack to active: %w", err)
	}

	f = nil
	return
}

// if it returns nil, it means there were not enough chunks to make a new pack
func (ds *dataStore) fillNewPack(packChunks map[uint64][]packer.ChunkInfoWithID) (newPack []packer.ChunkInfoWithID) {

	offset := int64(headerLen)

	var chunksAdded int

	var newPackFilled bool

	for packID, chunks := range packChunks {

		for _, ci := range chunks {

			size64 := int64(ci.SizeBytes)
			if offset+size64+prefixLen > ds.maxPackSize {
				newPackFilled = true
				break
			}

			newPack = append(newPack, ci)
			offset += size64 + prefixLen
			chunksAdded++
		}

		if chunksAdded == len(chunks) {
			delete(packChunks, packID)
			chunksAdded = 0
		} else if chunksAdded >= 1 {
			packChunks[packID] = chunks[chunksAdded-1:]
		}

		if newPackFilled {
			break
		}
	}

	if !newPackFilled {
		return nil
	}

	return
}
