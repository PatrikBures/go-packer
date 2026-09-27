package data

import (
	"errors"
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

func (ds *dataStore) FilterCompactable(packs map[uint64]packer.PackInfo) error {
	for packID, info := range packs {
		np := normalizePack(info)
		if ok, err := ds.shouldCompact(packID, np); err != nil {
			return fmt.Errorf("filtering packID %d: %w", packID, err)
		} else if !ok {
			delete(packs, packID)
		} else {
			packs[packID] = np
		}
	}
	return nil
}

func (ds *dataStore) shouldCompact(packID uint64, pack packer.PackInfo) (bool, error) {
	var size int64
	if f := ds.packRead.Get(packID); f != nil {
		stat, err := f.Stat()
		if err != nil {
			return false, fmt.Errorf("stating file from cache: %w", err)
		}
		size = stat.Size()
	} else {
		stat, err := os.Stat(ds.completedPackPath(packID))
		if errors.Is(err, os.ErrNotExist) {
			stat, err = os.Stat(ds.activePackPath(packID))
		}
		if err != nil {
			return false, fmt.Errorf("getting stat of pack: %w", err)
		}
		size = stat.Size()
	}
	return float64(size) / float64(pack.ChunkTotalSize) >= ds.compactPackRatio, nil
}

func normalizePack(pack packer.PackInfo) packer.PackInfo {
	pack.ChunkTotalSize += headerLen + (pack.ChunkCount * prefixLen)
	return pack
}

// Compacts next pack from chunks. Returns updated chunks whithout the chunks in newPackChunks.
func (ds *dataStore) CompactNextPack(chunks []packer.ChunkInfoWithID) (newChunks []packer.ChunkInfoWithID, newPackChunks []packer.ChunkInfoWithID, compactInfo packer.CompactInfo, err error) {

	newChunks, newPackChunks = ds.fillNewPack(chunks)
	if newPackChunks == nil {
		return
	}

	lock, packID, err := ds.lockActive(ds.nextPackID)
	if err != nil {
		err = fmt.Errorf("locking file: %w", err)
		return
	}
	defer func() {
		if !lock.Locked() {
			return
		}
		if err != nil {
			_ = lock.Unlock()
			return
		}
		err = lock.Unlock()
		if err != nil {
			err = fmt.Errorf("unlocking pack: %w", err)
		}
	}()

	completedPath := ds.completedPackPath(packID)

	if err = os.Rename(lock.Path(), completedPath); err != nil {
		_ = os.Remove(lock.Path())
		err = fmt.Errorf("moving pack to completed: %w", err)
		return
	}

	f, err := os.OpenFile(completedPath, os.O_TRUNC | os.O_WRONLY, 0666)
	if err != nil {
		err = fmt.Errorf("creating file: %w", err)
		return
	}
	defer func() {
		if f != nil && err != nil {
			_ = f.Close()
			_ = os.Remove(completedPath)
		}
	}()

	h, err := writePackHeader(packID, stateCompacting, f)
	if err != nil {
		err = fmt.Errorf("writing header to new pack: %w", err)
		return
	}

	offset := int64(headerLen)
	var buf []byte // should move to pool in future
	for i, ci := range newPackChunks {
		totalSize := ci.SizeBytes + prefixLen
		if len(buf) < int(totalSize) {
			buf = slices.Grow(buf, int(totalSize)-len(buf))[:totalSize:totalSize]
		}

		data, err := ds.Get(ci.ChunkInfo, buf)
		if err != nil {
			return nil, nil, packer.CompactInfo{}, fmt.Errorf("getting chunk: %w", err)
		}

		newCi, err := putChunk(packID, data, f, offset)
		if err != nil {
			return nil, nil, packer.CompactInfo{}, fmt.Errorf("puting chunk into new pack: %w", err)
		}

		offset += int64(totalSize)
		ci.ChunkInfo = newCi
		compactInfo.ChunksMoved++
		newPackChunks[i] = ci
	}

	compactInfo.PackBytesAdded = offset
	compactInfo.PacksCreated = 1

	h.State = stateCompleted

	if err := h.Update(f); err != nil {
		return nil, nil, packer.CompactInfo{}, fmt.Errorf("writing updated header: %w", err)
	}

	newPath := ds.completedPackPath(packID)
	if err := os.MkdirAll(filepath.Dir(newPath), 0775); err != nil {
		return nil, nil, packer.CompactInfo{}, err
	}

	if err = f.Sync(); err != nil {
		return nil, nil, packer.CompactInfo{}, fmt.Errorf("syncing file pack: %w", err)
	}

	if err = f.Close(); err != nil {
		return nil, nil, packer.CompactInfo{}, fmt.Errorf("closing pack file: %w", err)
	}

	f = nil
	return
}


// Returns remaining chunks and chunks to put into a new pack.
// If the latter slice is nil, it means there are not enough chunks to make a full pack.
//
// The provided slice is expected to be sorted by packID and bytes.
// Meaning that the chunks in smallest packID should be first.
func (ds *dataStore) fillNewPack(chunks []packer.ChunkInfoWithID) ([]packer.ChunkInfoWithID, []packer.ChunkInfoWithID) {
	var newPack []packer.ChunkInfoWithID

	offset := int64(headerLen)

	var newPackFilled bool

	var i int
	var ci packer.ChunkInfoWithID
	for i, ci = range chunks {

		size64 := int64(ci.SizeBytes)
		if offset+size64+prefixLen > ds.maxPackSize {
			newPackFilled = true
			break
		}

		newPack = append(newPack, ci)
		offset += size64 + prefixLen
	}

	if !newPackFilled {
		return chunks, nil
	}

	return chunks[i:], newPack
}
