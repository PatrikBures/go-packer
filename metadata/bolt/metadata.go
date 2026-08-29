// bolt implementation of the MetadataStore interface
package metadata

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

type metadataStore struct {
	db      *bolt.DB
	bufPool sync.Pool
}

func Init(path string) (*metadataStore, error) {
	db, err := bolt.Open(path, 0664, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	if err := migrateToVersion(db, 0); err != nil {
		return nil, fmt.Errorf("migrating db to latest version: %w", err)
	}

	return &metadataStore{
		db: db,
		bufPool: sync.Pool{
			New: func() any {
				return make([]byte, 64)
			},
		},
	}, nil
}

func (ms *metadataStore) Close() error {
	return ms.db.Close()
}

func (ms *metadataStore) getPool() []byte {
	return ms.bufPool.Get().([]byte)
}
func (ms *metadataStore) putPool(buf []byte) {
	ms.bufPool.Put(buf)
}

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


func (ms *metadataStore) Get(id []byte) (packer.ChunkInfo, error) {
	var ci packer.ChunkInfo

	if err := ms.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketChunks)
		if bucket == nil {
			return ErrBucketNotFound
		}

		v := bucket.Get(id)
		if v == nil {
			return ErrChunkNotFound
		}

		vci, err := packer.BytesToChunkInfo(v)
		if err != nil {
			return err
		}
		ci = vci

		return nil
	}); err != nil {
		return ci, err
	}

	return ci, nil
}

func (ms *metadataStore) Exists(id []byte) (bool, error) {
	var exists bool

	if err := ms.db.View(func(tx *bolt.Tx) error {
		bc := tx.Bucket(bucketChunks)
		if bc == nil {
			return ErrBucketNotFound
		}

		if bc.Get(id) != nil {
			exists = true
		}

		return nil
	}); err != nil {
		return false, err
	}

	return exists, nil
}

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

func (ms *metadataStore) Chunks(packIDs []uint64) ([]packer.ChunkInfo, error) {
	buf := ms.getPool()
	defer ms.putPool(buf)

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

func (ms *metadataStore) PackInfo(packIDs []uint64) (map[uint64]packer.PackInfo, error) {
	buf := ms.getPool()
	defer ms.putPool(buf)

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

		return cb.ForEach(func(k, v []byte) error {
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
				Id:        k,
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
		} else {
			return 1
		}
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

		buf := ms.getPool()
		defer ms.putPool(buf)

		for _, packID := range packs {
			binary.LittleEndian.PutUint64(buf, packID)

			if pb.Get(buf[:8]) == nil {
				packs[n] = packID
				n++
			}
		}
		fmt.Println()
		return nil
	}); err != nil {
		return nil, err
	}

	return packs[:n], nil
}

func (ms *metadataStore) version() (uint64, error) {
	var version uint64
	return version, ms.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketVersion)
		if bucket == nil {
			return ErrBucketNotFound
		}
		v := bucket.Get(keyVersion)
		if v != nil {
			version = binary.LittleEndian.Uint64(v)
		}
		return nil
	})
}
