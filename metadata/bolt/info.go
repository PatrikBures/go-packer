package metadata

import (
	"bytes"
	"encoding/binary"
	"slices"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

func (ms *metadataStore) Chunks(packIDs []uint64) ([]packer.ChunkInfo, error) {
	packsMap := sliceToMapUint64(packIDs)

	chunks := make([]packer.ChunkInfo, 0, 128)

	if err := ms.db.View(func(tx *bolt.Tx) error {
		cb := tx.Bucket(bucketChunks)
		if cb == nil {
			return ErrBucketNotFound
		}

		return cb.ForEach(func(k, v []byte) error {
			if len(v) < packer.ChunkInfoSize {
				return nil
			}

			packID := binary.LittleEndian.Uint64(v[0:8])
			if _, ok := packsMap[packID]; !ok {
				return nil
			}

			ci, err := packer.BytesToChunkInfo(v)
			if err != nil {
				return err
			}

			chunks = append(chunks, ci)
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return chunks, nil
}

func (ms *metadataStore) ChunkIDsAll() ([][]byte, error) {
	chunkIDs := make([][]byte, 0, 128)
	return chunkIDs, ms.db.View(func(tx *bolt.Tx) error {
		cb := tx.Bucket(bucketChunks)
		if cb == nil {
			return ErrBucketNotFound
		}
		return cb.ForEach(func(k, _ []byte) error {
			chunkIDs = append(chunkIDs, bytes.Clone(k))
			return nil
		})
	})
}

func (ms *metadataStore) PackInfo(packIDs []uint64) (map[uint64]packer.PackInfo, error) {
	packInfos := make(map[uint64]packer.PackInfo)

	packsMap := sliceToMapUint64(packIDs)
	if err := ms.db.View(func(tx *bolt.Tx) error {
		cb := tx.Bucket(bucketChunks)
		if cb == nil {
			return ErrBucketNotFound
		}

		return cb.ForEach(func(k, v []byte) error {
			if len(v) < packer.ChunkInfoSize {
				return nil
			}
			packID := binary.LittleEndian.Uint64(v[0:8])
			if _, ok := packsMap[packID]; !ok {
				return nil
			}

			size := binary.LittleEndian.Uint32(v[16:20])

			pv := packInfos[packID]
			pv.ChunkCount++
			pv.ChunkTotalSize += uint64(size)
			packInfos[packID] = pv

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return packInfos, nil
}

func (ms *metadataStore) PackInfoAll() (map[uint64]packer.PackInfo, error) {
	packInfos := make(map[uint64]packer.PackInfo)

	if err := ms.db.View(func(tx *bolt.Tx) error {
		cb := tx.Bucket(bucketChunks)
		if cb == nil {
			return ErrBucketNotFound
		}

		return cb.ForEach(func(_, v []byte) error {
			if len(v) < packer.ChunkInfoSize {
				return nil
			}
			packID := binary.LittleEndian.Uint64(v[0:8])
			size := binary.LittleEndian.Uint32(v[16:20])

			pv := packInfos[packID]
			pv.ChunkCount++
			pv.ChunkTotalSize += uint64(size)
			packInfos[packID] = pv

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return packInfos, nil
}

func (ms *metadataStore) ChunksForPacks(packs map[uint64]packer.PackInfo) ([]packer.ChunkInfoWithID, error) {
	chunks := make([]packer.ChunkInfoWithID, 0, len(packs)*5)

	if err := ms.db.View(func(tx *bolt.Tx) error {
		cb := tx.Bucket(bucketChunks)
		if cb == nil {
			return ErrBucketNotFound
		}

		return cb.ForEach(func(k, v []byte) error {
			if len(v) < packer.ChunkInfoSize {
				return nil
			}

			packID := binary.LittleEndian.Uint64(v[0:8])
			if _, ok := packs[packID]; !ok {
				return nil
			}

			ci, _ := packer.BytesToChunkInfo(v)
			ciw := packer.ChunkInfoWithID{
				ChunkInfo: ci,
				Id:        bytes.Clone(k),
			}

			chunks = append(chunks, ciw)

			return nil
		})
	}); err != nil {
		return nil, err
	}
	slices.SortFunc(chunks, func(a packer.ChunkInfoWithID, b packer.ChunkInfoWithID) int {
		if a.PackID < b.PackID {
			return -1
		}
		return 1
	})

	return chunks, nil
}

func (ms *metadataStore) FilterExistingPacks(packs []uint64) ([]uint64, error) {
	var n int
	if err := ms.db.Update(func(tx *bolt.Tx) error {
		pb := tx.Bucket(bucketPacks)
		if pb == nil {
			return ErrBucketNotFound
		}

		var packIDBytes [8]byte

		for _, packID := range packs {
			binary.LittleEndian.PutUint64(packIDBytes[:], packID)

			if pb.Get(packIDBytes[:]) == nil {
				packs[n] = packID
				n++
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return packs[:n], nil
}
