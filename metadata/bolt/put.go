package metadata

import (
	"encoding/binary"

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

		buf := ms.getPool()
		defer ms.putPool(buf)

		return addChunk(bc, bp, buf, id, ci)

	}); err != nil {
		return err
	}

	return nil
}

func (ms *metadataStore) UpdateChunks(chunks []packer.ChunkInfoWithID) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		bc := tx.Bucket(bucketChunks)
		if bc == nil {
			return ErrBucketNotFound
		}
		bp := tx.Bucket(bucketPacks)
		if bp == nil {
			return ErrBucketNotFound
		}

		buf := ms.getPool()
		defer ms.putPool(buf)

		for _, ci := range chunks {
			if err := addChunk(bc, bp, buf, ci.Id, ci.ChunkInfo); err != nil {
				return err
			}
		}
		return nil
	})
}

// Puts chunk and puts pack increasing the refcount.
// If the chunk already exists, and its existing packID differs from the new one,
// its refcount is reduced, if the refcount would become negative, that pack is deleted.
func addChunk(bc *bolt.Bucket, bp *bolt.Bucket, buf []byte, id []byte, ci packer.ChunkInfo) error {
	packIDBytes := buf[:8]
	binary.LittleEndian.PutUint64(packIDBytes, ci.PackID)

	refsBytes := bp.Get(packIDBytes)
	var refs uint32
	if refsBytes != nil {
		refs = binary.LittleEndian.Uint32(refsBytes)
	}
	existingChunkBytes := bc.Get(id)
	if len(existingChunkBytes) == 20 {

		existingPackID := binary.LittleEndian.Uint64(existingChunkBytes[:8])

		if existingPackID != ci.PackID {
			existingPackIDBytes := buf[32:40]
			binary.LittleEndian.PutUint64(existingPackIDBytes, existingPackID)

			existingRefsBytes := bp.Get(existingPackIDBytes)
			existingRefs := binary.LittleEndian.Uint32(existingRefsBytes)

			if existingRefs == 1 {
				if err := bp.Delete(existingPackIDBytes); err != nil {
					return err
				}
			} else {
				binary.LittleEndian.PutUint32(existingRefsBytes, existingRefs-1)
				if err := bp.Put(existingPackIDBytes, existingRefsBytes); err != nil {
					return err
				}
			}
		}
	}

	newRefsBytes := buf[8:12]
	binary.LittleEndian.PutUint32(newRefsBytes, refs+1)

	if err := bp.Put(packIDBytes, newRefsBytes); err != nil {
		return err
	}

	chunkInfoBytes := buf[12:32]
	ci.PutBytes(buf[12:32])

	return bc.Put(id, chunkInfoBytes)
}
