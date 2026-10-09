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
		bc := tx.Bucket(bucketChunks)
		if bc == nil {
			return ErrBucketNotFound
		}
		bp := tx.Bucket(bucketPacks)
		if bp == nil {
			return ErrBucketNotFound
		}

		return addChunk(bc, bp, id, ci)

	}); err != nil {
		return err
	}

	return nil
}

func (ms *metadataStore) PutChunks(chunks []packer.ChunkInfoWithID) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		bc := tx.Bucket(bucketChunks)
		if bc == nil {
			return ErrBucketNotFound
		}
		bp := tx.Bucket(bucketPacks)
		if bp == nil {
			return ErrBucketNotFound
		}

		for _, ci := range chunks {
			if err := addChunk(bc, bp, ci.Id, ci.ChunkInfo); err != nil {
				return err
			}
		}
		return nil
	})
}

// Puts chunk and puts pack increasing the refcount.
// If the chunk already exists, and its existing packID differs from the new one,
// its refcount is reduced, if the refcount would become negative, that pack is deleted.
func addChunk(bc *bolt.Bucket, bp *bolt.Bucket, id []byte, ci packer.ChunkInfo) error {

	var packIDKey [8]byte
	binary.LittleEndian.PutUint64(packIDKey[:], ci.PackID)

	existingChunkBytes := bc.Get(id)

	if existingChunkBytes != nil {
		if len(existingChunkBytes) != packer.ChunkInfoSize {
			return fmt.Errorf("invalid chunk info, id: %x, len: %d, want len: %d", id, len(existingChunkBytes), packer.ChunkInfoSize)
		}

		existingPackID := binary.LittleEndian.Uint64(existingChunkBytes)

		var want [packer.ChunkInfoSize]byte
		ci.PutBytes(want[:])
		// nothing to do if chunk already exists and packID is same
		if existingPackID == ci.PackID && bytes.Equal(existingChunkBytes, want[:]) {
			return nil
		}

		var existingPackIDKey [8]byte
		binary.LittleEndian.PutUint64(existingPackIDKey[:], existingPackID)

		existingRefsBytesReceived := bp.Get(existingPackIDKey[:])
		if existingRefsBytesReceived == nil {
			return fmt.Errorf("pack not found: %d", existingPackID)
		}
		if len(existingRefsBytesReceived) != 4 {
			return fmt.Errorf("invalid refcount len %d for pack %d", len(existingRefsBytesReceived), existingPackID)
		}
		existingRefs := binary.LittleEndian.Uint32(existingRefsBytesReceived)

		switch {
		case existingRefs == 0:
			return fmt.Errorf("invalid refcount %d for pack %d", existingRefs, existingPackID)
		case existingRefs == 1:
			fmt.Printf("TESTING, deleting packID: %d\n", existingPackID)
			if err := bp.Delete(existingPackIDKey[:]); err != nil {
				return err
			}
		default:
			var decRefs [4]byte
			binary.LittleEndian.PutUint32(decRefs[:], existingRefs-1)

			if err := bp.Put(existingPackIDKey[:], decRefs[:]); err != nil {
				return err
			}
		}
	}

	var (
		refsVal [4]byte
		infoVal [packer.ChunkInfoSize]byte
	)

	refsBytes := bp.Get(packIDKey[:])
	var refs uint32
	if refsBytes != nil {
		if len(refsBytes) != 4 {
			return fmt.Errorf("invalid refcount len %d for pack %d", len(refsBytes), ci.PackID)
		}
		refs = binary.LittleEndian.Uint32(refsBytes)
	}

	binary.LittleEndian.PutUint32(refsVal[:], refs+1)
	ci.PutBytes(infoVal[:])

	if err := bp.Put(packIDKey[:], refsVal[:]); err != nil {
		return err
	}

	return bc.Put(id, infoVal[:])
}
