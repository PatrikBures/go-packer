package metadata

import (
	"encoding/binary"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

func (ms *metadataStore) Delete(id []byte) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		bc := tx.Bucket(bucketChunks)
		if bc == nil {
			return ErrBucketNotFound
		}

		chunkInfoBytes := bc.Get(id)
		if chunkInfoBytes == nil {
			return nil
		}

		bp := tx.Bucket(bucketPacks)
		if bp == nil {
			return ErrBucketNotFound
		}

		buf := ms.getPool()
		defer ms.putPool(buf)

		chunkInfo, err := packer.BytesToChunkInfo(chunkInfoBytes)
		if err != nil {
			return err
		}

		packIDBytes := buf[:8]
		binary.LittleEndian.PutUint64(packIDBytes, chunkInfo.PackID)

		refsBytes := bp.Get(packIDBytes)
		if refsBytes == nil {
			return bc.Delete(id)
		}

		refs := binary.LittleEndian.Uint32(refsBytes)
		newRefsBytes := buf[8:12]
		binary.LittleEndian.PutUint32(newRefsBytes, refs-1)

		if err := bp.Put(packIDBytes, newRefsBytes); err != nil {
			return err
		}

		return bc.Delete(id)
	})
}
