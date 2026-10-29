package metadata

import (
	"bytes"
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

func (ms *metadataStore) Put(id []byte, ci packer.ChunkInfo) error {
	if err := ms.db.Update(func(tx *bolt.Tx) error {
		buckets, err := openAddChunkBuckets(tx)
		if err != nil {
			return err
		}
		return addChunk(buckets, id, ci)

	}); err != nil {
		return fmt.Errorf("adding chunk: %w", err)
	}

	return nil
}

func (ms *metadataStore) PutChunks(chunks []packer.ChunkInfoWithID) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		for i, ci := range chunks {
			buckets, err := openAddChunkBuckets(tx)
			if err != nil {
				return err
			}
			if err := addChunk(buckets, ci.Id, ci.ChunkInfo); err != nil {
				return fmt.Errorf("adding chunk %d/%d: %w", i+1, len(chunks), err)
			}
		}
		return nil
	})
}

type addChunkBuckets struct {
	chunks      *bolt.Bucket
	packs       *bolt.Bucket
	packsDelete *bolt.Bucket
}

func openAddChunkBuckets(tx *bolt.Tx) (*addChunkBuckets, error) {
	chunks := tx.Bucket(bucketChunks)
	if chunks == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, bucketChunks)
	}
	packs := tx.Bucket(bucketPacks)
	if packs == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, bucketPacks)
	}
	packsDelete := tx.Bucket(bucketPacksDelete)
	if packsDelete == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, bucketPacksDelete)
	}

	return &addChunkBuckets{
		chunks:      chunks,
		packs:       packs,
		packsDelete: packsDelete,
	}, nil
}

// Puts chunk and puts pack increasing the refcount.
// If the chunk already exists, and its existing packID differs from the new one,
// its refcount is reduced, if the refcount would become negative, that pack is deleted.
//
// it does not open the buckets, as this functions is also used in a loop.
// if it would open the buckets it would then do it each iteration.
func addChunk(bucket *addChunkBuckets, id []byte, ci packer.ChunkInfo) error {

	var packIDKey [8]byte
	binary.LittleEndian.PutUint64(packIDKey[:], ci.PackID)

	// ensure that pack already is not marked for deletion
	if bucket.packsDelete.Get(packIDKey[:]) != nil {
		return fmt.Errorf("attempting to write chunk to pack %d marked for deletion", ci.PackID)
	}

	// if chunk already exists
	if existingChunkBytes := bucket.chunks.Get(id); existingChunkBytes != nil {
		if len(existingChunkBytes) != packer.ChunkInfoSize {
			return fmt.Errorf("invalid chunk info, id: %x, len: %d, want len: %d", id, len(existingChunkBytes), packer.ChunkInfoSize)
		}


		// if same pack
		existingPackID := binary.LittleEndian.Uint64(existingChunkBytes)
		if existingPackID == ci.PackID {
			var want [packer.ChunkInfoSize]byte
			ci.PutBytes(want[:])
			if bytes.Equal(existingChunkBytes, want[:]) {
				return nil
			}
			if err := bucket.chunks.Put(id, want[:]); err != nil {
				return fmt.Errorf("updating chunk %x: %w", id, err)
			}
			return nil
		}

		// if different pack
		var existingPackIDKey [8]byte
		binary.LittleEndian.PutUint64(existingPackIDKey[:], existingPackID)
		packInfoValue := bucket.packs.Get(existingPackIDKey[:])
		if packInfoValue == nil {
			return fmt.Errorf("pack not found: %d", existingPackID)
		}

		packInfo, err := packer.BytesToPackInfo(packInfoValue)
		if err != nil {
			return fmt.Errorf("converting bytes to PackInfo, for existing pack: %w", err)
		}

		packInfo.Size -= uint64(ci.Size)
		if err := decPack(bucket, packInfo, existingPackID, existingPackIDKey); err != nil {
			return fmt.Errorf("updating existing pack: %w", err)
		}
	}


	// update pack
	var packInfo packer.PackInfo
	if packInfoBytes := bucket.packs.Get(packIDKey[:]); packInfoBytes != nil {
		var err error
		packInfo, err = packer.BytesToPackInfo(packInfoBytes)
		if err != nil {
			return fmt.Errorf("converting bytes to PackInfo, for new pack: %w", err)
		}
	} else {
		packInfo.State = packer.PackStateActive
	}

	packInfo.ChunkCount++
	packInfo.Size += uint64(ci.Size)

	if err := bucket.packs.Put(packIDKey[:], packInfo.Bytes()); err != nil {
		return fmt.Errorf("updating pack %d refs: %w", ci.PackID, err)
	}

	if err := bucket.chunks.Put(id, ci.Bytes()); err != nil {
		return fmt.Errorf("adding chunk: %w", err)
	}

	return nil
}



func (ms *metadataStore) MarkCompleted(packID uint64) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		bp := tx.Bucket(bucketPacks)
		if bp == nil {
			return ErrBucketNotFound
		}

		var packIDKey [8]byte
		binary.LittleEndian.PutUint64(packIDKey[:], packID)

		packIDVal := bp.Get(packIDKey[:])
		if packIDVal == nil {
			return fmt.Errorf("%w: %d", packer.ErrPackNotFound, packID)
		}
		var packIDBytes [packer.PackInfoSize]byte
		copy(packIDVal, packIDBytes[:])
		packIDBytes[packer.PackInfoIdxState] = byte(packer.PackStateCompleted)

		return bp.Put(packIDKey[:], packIDBytes[:])
	})
}
