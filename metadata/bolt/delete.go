package metadata

import (
	"encoding/binary"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

func (ms *metadataStore) Delete(ids [][]byte) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		bc := tx.Bucket(bucketChunks)
		if bc == nil {
			return ErrBucketNotFound
		}

		bp := tx.Bucket(bucketPacks)
		if bp == nil {
			return ErrBucketNotFound
		}

		for _, id := range ids {

			chunkInfoBytes := bc.Get(id)
			if chunkInfoBytes == nil {
				return nil
			}

			chunkInfo, err := packer.BytesToChunkInfo(chunkInfoBytes)
			if err != nil {
				return err
			}

			var packIDBytes [8]byte
			binary.LittleEndian.PutUint64(packIDBytes[:], chunkInfo.PackID)

			refsBytes := bp.Get(packIDBytes[:])
			if refsBytes == nil {
				return bc.Delete(id)
			}

			refs := binary.LittleEndian.Uint32(refsBytes)
			newRefsBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(newRefsBytes, refs-1)

			if err := bp.Put(packIDBytes[:], newRefsBytes); err != nil {
				return err
			}

			if err := bc.Delete(id); err != nil {
				return err
			}
		}

		return nil
	})
}
