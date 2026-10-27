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

			refsBytes := bucket.packs.Get(packIDKey[:])
			if refsBytes == nil {
				if err := bucket.chunks.Delete(id); err != nil {
					return fmt.Errorf("deleting orphaned chunk: %w", err)
				}
				continue
			}

			refs := binary.LittleEndian.Uint32(refsBytes)

			if err := updateOrDeletePack(bucket, refs, chunkInfo.PackID, packIDKey); err != nil {
				return fmt.Errorf("deleting or updating pack: %w", err)
			}

			if err := bucket.chunks.Delete(id); err != nil {
				return fmt.Errorf("deleting chunk: %w", err)
			}
		}

		return nil
	})
}

// decrements pack count by one.
// refCount == 1 -> pack goes to packDelete
// refCount >  1 -> updates stored refcount to refCount-1
// refCount == 0 -> error
func updateOrDeletePack(bucket *addChunkBuckets, refCount uint32, packID uint64, packIDKey [8]byte) error {

	switch refCount {
	case 0:
		return fmt.Errorf("invalid refcount %d for pack %d", refCount, packID)
	case 1:
		if err := bucket.packs.Delete(packIDKey[:]); err != nil {
			return fmt.Errorf("deleting unreferenced pack %d: %w", packID, err)
		}
		if err := bucket.packsDelete.Put(packIDKey[:], []byte{}); err != nil {
			return fmt.Errorf("adding deleted pack %d to pending: %w", packID, err)
		}
	default:
		var decRefs [4]byte
		binary.LittleEndian.PutUint32(decRefs[:], refCount-1)

		if err := bucket.packs.Put(packIDKey[:], decRefs[:]); err != nil {
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
