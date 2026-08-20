// bolt implementation of the MetadataStore interface
package metadata

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

type metadataStore struct {
	db *bolt.DB
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

		packIDBytes := buf[:8]
		binary.LittleEndian.PutUint64(packIDBytes, ci.PackID)

		refsBytes := bp.Get(packIDBytes)
		var refs uint32
		if refsBytes != nil {
			refs = binary.LittleEndian.Uint32(refsBytes)
		}

		newRefsBytes := buf[8:12]
		binary.LittleEndian.PutUint32(newRefsBytes, refs+1)

		if err := bp.Put(packIDBytes, newRefsBytes); err != nil {
			return err
		}

		return bc.Put(id, ci.PutBytes(buf[12:32]))
	}); err != nil {
		return err
	}

	return nil
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
		binary.LittleEndian.PutUint32(newRefsBytes, refs -1)

		if err := bp.Put(packIDBytes, newRefsBytes); err != nil {
			return err
		}

		return bc.Delete(id)
	})
}

func (ms *metadataStore) Packs() ([]uint64, error) {
	var packs []uint64

	if err := ms.db.View(func(tx *bolt.Tx) error {
		bp := tx.Bucket(bucketPacks)
		if bp == nil {
			return ErrBucketNotFound
		}

		_ = bp.ForEach(func(k, v []byte) error {
			packs = append(packs, binary.LittleEndian.Uint64(k))
			return nil
		})

		return nil
	}); err != nil {
		return nil, err
	}
	
	return packs, nil
}

func (ms *metadataStore) ChunkCount(uint64) (uint64, error) {
	return 0, nil
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
