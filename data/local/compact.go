package data

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"go.pabu.dev/packer"
)

// Updates the total size to the expected file size including header and all prefixes.
func (ds *dataStore) NormalizePackSize(packSize uint64, chunkCount uint32) uint64 {
	return packSize + headerLen + (uint64(chunkCount) * prefixLen)
}

func (ds *dataStore) FilterCompactable(packs map[uint64]packer.PackInfo) error {
	for packID, info := range packs {
		info.Size = ds.NormalizePackSize(info.Size, info.ChunkCount)
		if ok, err := ds.shouldCompact(packID, info); err != nil {
			return fmt.Errorf("filtering packID %d: %w", packID, err)
		} else if !ok {
			delete(packs, packID)
		} else {
			packs[packID] = info
		}
	}
	return nil
}

func (ds *dataStore) shouldCompact(packID uint64, pack packer.PackInfo) (bool, error) {
	if packID == ds.currentPackID || pack.State == packer.PackStateActive {
		return false, nil
	}

	stat, err := os.Stat(ds.completedPackPath(packID))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stating completed pack %d: %w", packID, err)
	}
	size := stat.Size()

	ratio := float64(pack.Size) / float64(size)
	ok := ratio <= ds.compactPackRatio
	// fmt.Printf("shouldCompact: pack=%d\tlive=%d\tphysical=%d\tratio=%.3f limit=%.3f -> %v\n",
	//        packID, pack.Size, size, ratio, ds.compactPackRatio, ok)
	return ok, nil
}

func (ds *dataStore) CompactToPack(packID uint64, chunks []packer.ChunkInfoWithID) (compactInfo packer.CompactInfo, err error) {

	locked, err := ds.tryLockPack(packID)
	if err != nil {
		return packer.CompactInfo{}, fmt.Errorf("locking file: %w", err)
	}
	if !locked {
		return packer.CompactInfo{}, fmt.Errorf("did not get lock: %s", ds.activePackPath(packID))
	}
	defer func() {
		if lerr := ds.unlockPack(packID); lerr != nil {
			err = errors.Join(err, fmt.Errorf("unlocking pack: %w", lerr))
		}
	}()

	packPath := ds.activePackPath(packID)

	f, err := os.OpenFile(packPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return packer.CompactInfo{}, fmt.Errorf("creating file: %w", err)
	}
	defer func() {
		if err == nil {
			return
		}
		_ = f.Close()
		_ = os.Remove(packPath)
	}()

	h, err := writePackHeader(packID, stateCompacting, f)
	if err != nil {
		return packer.CompactInfo{}, fmt.Errorf("writing header to new pack: %w", err)
	}

	offset := int64(headerLen)
	var buf []byte // should move to pool in future
	for i, ci := range chunks {
		totalSize := ci.Size + prefixLen
		if len(buf) < int(totalSize) {
			buf = slices.Grow(buf, int(totalSize)-len(buf))[:totalSize:totalSize]
		}

		data, err := ds.Get(ci.ChunkInfo, buf)
		if err != nil {
			return packer.CompactInfo{}, fmt.Errorf("getting chunk: %w", err)
		}

		newCi, err := putChunk(packID, data, f, offset)
		if err != nil {
			return packer.CompactInfo{}, fmt.Errorf("puting chunk into new pack: %w", err)
		}

		offset += int64(totalSize)
		ci.ChunkInfo = newCi
		compactInfo.ChunksMoved++
		chunks[i] = ci
	}

	compactInfo.PackBytesAdded = offset
	compactInfo.PacksCreated = 1

	h.State = stateCompleted

	if err = h.WriteAtFile(f); err != nil {
		return packer.CompactInfo{}, fmt.Errorf("writing updated header: %w", err)
	}

	if err = f.Sync(); err != nil {
		return packer.CompactInfo{}, fmt.Errorf("syncing file pack: %w", err)
	}

	if err = f.Close(); err != nil {
		return packer.CompactInfo{}, fmt.Errorf("closing pack file: %w", err)
	}

	if err = os.MkdirAll(ds.completedPackDirPath(packID), 0775); err != nil {
		return packer.CompactInfo{}, err
	}

	completedPath := ds.completedPackPath(packID)
	if err = os.Rename(packPath, completedPath); err != nil {
		return packer.CompactInfo{}, fmt.Errorf("moving pack to completed: %w", err)
	}

	return
}

// Returns remaining chunks and chunks to put into a new pack.
// If the latter slice is nil, it means there are not enough chunks to make a full pack.
//
// The provided slice is expected to be sorted by packID and bytes.
// Meaning that the chunks in smallest packID should be first.
func (ds *dataStore) FillNewPack(chunks []packer.ChunkInfoWithID) ([]packer.ChunkInfoWithID, []packer.ChunkInfoWithID) {
	var newChunks []packer.ChunkInfoWithID

	offset := int64(headerLen)

	var newChunksFilled bool
	packIDs := make(map[uint64]struct{})

	var i int
	var ci packer.ChunkInfoWithID
	for i, ci = range chunks {

		size64 := int64(ci.Size)
		if offset+size64+prefixLen > ds.maxPackSize {
			newChunksFilled = true
			break
		}

		packIDs[ci.PackID] = struct{}{}
		newChunks = append(newChunks, ci)
		offset += size64 + prefixLen
	}
	fmt.Printf("fillNewPack:  len chunks=%d, len newChunks=%d\n", len(chunks), len(newChunks))

	if !newChunksFilled {
		if len(packIDs) < 5 {
			fmt.Printf("!fillNewPack: offset=%d, maxPackSize=%d\n", offset, ds.maxPackSize)
			return chunks, nil
		}
		// no remaining chunks
		return nil, newChunks
	}

	return chunks[i:], newChunks
}
