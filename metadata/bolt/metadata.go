// bolt implementation of the MetadataStore interface
package metadata

import (
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

type metadataStore struct {
	db *bolt.DB
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
	}, nil
}

func (ms *metadataStore) Close() error {
	return ms.db.Close()
}

func (ms *metadataStore) Put(id []byte, ci packer.ChunkInfo) error {
	if err := ms.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketChunks)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Put(id, ci.Bytes())
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

func (ms *metadataStore) Delete(id []byte) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketChunks)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Delete(id)
	})
}

func (ms *metadataStore) Version() (uint64, error) {
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
