package metadata

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

func (ms *metadataStore) Delete(ids [][]byte) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		bucket, err := openAddChunkBuckets(tx)
		if err != nil {
			return err
		}

		for _, id := range ids {

			chunkInfoBytes := bucket.chunks.Get(id)
			if chunkInfoBytes == nil {
				continue
			}

			chunkInfo, err := packer.BytesToChunkInfo(chunkInfoBytes)
			if err != nil {
				return err
			}

			var packIDKey [8]byte
			binary.LittleEndian.PutUint64(packIDKey[:], chunkInfo.PackID)

			packInfoValue := bucket.packs.Get(packIDKey[:])
			if packInfoValue == nil {
				if err := bucket.chunks.Delete(id); err != nil {
					return fmt.Errorf("deleting orphaned chunk: %w", err)
				}
				continue
			}

			packInfo, err := packer.BytesToPackInfo(packInfoValue)
			if err != nil {
				return fmt.Errorf("converting bytes to PackInfo while deleting chunks: %w", err)
			} 

			packInfo.ChunkTotalSize -= uint64(chunkInfo.SizeBytes)
			if err := decPack(bucket, packInfo, chunkInfo.PackID, packIDKey); err != nil {
				return fmt.Errorf("deleting or updating pack: %w", err)
			}

			if err := bucket.chunks.Delete(id); err != nil {
				return fmt.Errorf("deleting chunk: %w", err)
			}
		}

		return nil
	})
}

// Decrements pack count by one. Remember to update ChunkTotalSize before running this, but not the ChunkCount!
//
// ChunkCount is 1 and the pack is Completed -> pack goes to packDelete
// 
// ChunkCount larger than 1 -> decreases ChunkCount by one
func decPack(bucket *addChunkBuckets, packInfo packer.PackInfo, packID uint64, packIDKey [8]byte) error {

	if packInfo.State == packer.PackStateCompleted && packInfo.ChunkCount <= 1 {
		if err := bucket.packs.Delete(packIDKey[:]); err != nil {
			return fmt.Errorf("deleting unreferenced pack %d: %w", packID, err)
		}
		if err := bucket.packsDelete.Put(packIDKey[:], []byte{}); err != nil {
			return fmt.Errorf("adding deleted pack %d to pending: %w", packID, err)
		}
	} else if packInfo.ChunkCount == 0 {
		return fmt.Errorf("Can not decrease ChunkCount below 0. This error should never occur")
	} else {
		packInfo.ChunkCount--

		if err := bucket.packs.Put(packIDKey[:], packInfo.Bytes()); err != nil {
			return fmt.Errorf("decreasing refs for pack %d: %w", packID, err)
		}
	}

	return nil
}

func (ms *metadataStore) PacksDeleteBatch() ([]uint64, error) {
	deleteBatchQty := 10

	packIDs := make([]uint64, 0, deleteBatchQty)

	if err := ms.db.Update(func(tx *bolt.Tx) error {
		bpd := tx.Bucket(bucketPacksDelete)
		if bpd == nil {
			return ErrBucketNotFound
		}

		if err := bpd.ForEach(func(k, _ []byte) error {
			packID := binary.LittleEndian.Uint64(k)
			packIDs = append(packIDs, packID)

			if err := bpd.Delete(k); err != nil {
				return err
			}

			if len(packIDs) >= deleteBatchQty {
				return io.EOF
			}

			return nil
		}); err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return packIDs, nil
}
