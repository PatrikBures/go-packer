package metadata

import (
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

	var packIDBuf[8]byte
	binary.LittleEndian.PutUint64(packIDBuf[:], ci.PackID)

	refsBytes := bp.Get(packIDBuf[:])
	var refs uint32
	if refsBytes != nil {
		refs = binary.LittleEndian.Uint32(refsBytes)
	}
	existingChunkBytes := bc.Get(id)
	if len(existingChunkBytes) == 20 {

		existingPackID := binary.LittleEndian.Uint64(existingChunkBytes)

		// if existing pack id differs from provided one.
		// it reduces the ref count by one, if it would become zero, it deletes that pack.
		if existingPackID != ci.PackID {
			var existingPackIDBuf [8]byte
			binary.LittleEndian.PutUint64(existingPackIDBuf[:], existingPackID)

			existingRefsBytesReceived := bp.Get(existingPackIDBuf[:])
			if existingRefsBytesReceived == nil {
				return fmt.Errorf("pack not found: %d", existingPackID)
			}
			existingRefs := binary.LittleEndian.Uint32(existingRefsBytesReceived)

			if existingRefs == 1 {
				if err := bp.Delete(existingPackIDBuf[:]); err != nil {
					return err
				}
			} else {
				decBuf := make([]byte, 12)

				decPackID := decBuf[:8]
				binary.LittleEndian.PutUint64(decPackID, existingPackID)

				decRefs := decBuf[8:12]
				binary.LittleEndian.PutUint32(decRefs, existingRefs-1)

				if err := bp.Put(decPackID, decRefs); err != nil {
					return err
				}
			}
		}
	}

	packBuf := make([]byte, 12)

	packIDKey := packIDBuf[:8]
	binary.LittleEndian.PutUint64(packIDKey, ci.PackID)

	newRefsVal := packBuf[8:12]
	binary.LittleEndian.PutUint32(newRefsVal, refs+1)

	if err := bp.Put(packIDKey, newRefsVal); err != nil {
		return err
	}

	chunkInfoVal := make([]byte, 20)
	ci.PutBytes(chunkInfoVal)

	return bc.Put(id, chunkInfoVal)
}
