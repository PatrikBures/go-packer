package metadata

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"slices"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

func (ms *metadataStore) Chunks(packIDs []uint64) ([]packer.ChunkInfo, error) {
	if packIDs == nil {
		return []packer.ChunkInfo{}, nil
	}
	return ms.chunks(packIDs)
}

func (ms *metadataStore) ChunksAll() ([]packer.ChunkInfo, error) {
	return ms.chunks(nil)
}

// if packids is nil, it will return ALL chunks.
func (ms *metadataStore) chunks(packIDs []uint64) ([]packer.ChunkInfo, error) {

	chunks := make([]packer.ChunkInfo, 0, 128)

	var fn func(k, v []byte) error
	if packIDs == nil {
		fn = func(k, v []byte) error {
			if len(v) < packer.ChunkInfoSize {
				return nil
			}

			ci, err := packer.BytesToChunkInfo(v)
			if err != nil {
				return err
			}

			chunks = append(chunks, ci)
			return nil
		}
	} else {
		fn = func(k, v []byte) error {
			if len(v) < packer.ChunkInfoSize {
				return nil
			}
			packsMap := sliceToMapUint64(packIDs)

			packID := binary.LittleEndian.Uint64(v[packer.CI1PackID:packer.CI2Offset])
			if _, ok := packsMap[packID]; !ok {
				return nil
			}

			ci, err := packer.BytesToChunkInfo(v)
			if err != nil {
				return err
			}

			chunks = append(chunks, ci)
			return nil
		}
	}

	if err := ms.db.View(func(tx *bolt.Tx) error {
		cb := tx.Bucket(bucketChunks)
		if cb == nil {
			return ErrBucketNotFound
		}
		return cb.ForEach(fn)
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
	if packIDs == nil {
		return make(map[uint64]packer.PackInfo), nil
	}
	return ms.packInfo(packIDs)
}

func (ms *metadataStore) PackInfoAll() (map[uint64]packer.PackInfo, error) {
	return ms.packInfo(nil)
}

func (ms *metadataStore) packInfo(packIDs []uint64) (map[uint64]packer.PackInfo, error) {
	packInfos := make(map[uint64]packer.PackInfo)

	var packsMap map[uint64]struct{}
	if packIDs != nil {
		packsMap = sliceToMapUint64(packIDs)
	}
	if err := ms.db.View(func(tx *bolt.Tx) error {
		cb := tx.Bucket(bucketChunks)
		if cb == nil {
			return ErrBucketNotFound
		}
		cp := tx.Bucket(bucketPacks)
		if cb == nil {
			return ErrBucketNotFound
		}

		var f func(k, v []byte) error
		if packsMap == nil {
			f = func(k, v []byte) error {
				if len(v) < packer.PackInfoSize {
					return nil
				}

				packID := binary.LittleEndian.Uint64(k[0:8])

				packInfo, err := packer.BytesToPackInfo(v)
				if err != nil {
					return err
				}
				packInfos[packID] = packInfo

				return nil
			}
		} else {
			f = func(k, v []byte) error {
				if len(v) < packer.PackInfoSize {
					return nil
				}

				packID := binary.LittleEndian.Uint64(k[0:8])

				if _, ok := packsMap[packID]; !ok {
					return nil
				}

				packInfo, err := packer.BytesToPackInfo(v)
				if err != nil {
					return err
				}
				packInfos[packID] = packInfo

				return nil
			}
		}

		return cp.ForEach(f)

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

			packID := binary.LittleEndian.Uint64(v[packer.CI1PackID:packer.CI2Offset])
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
		return cmp.Compare(a.PackID, b.PackID)
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
