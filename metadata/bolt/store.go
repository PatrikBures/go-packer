// bolt implementation of the MetadataStore interface
package metadata

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type metadataStore struct {
	db      *bolt.DB

	// TODO: Remove buf as small buffers like this are not worth
	// it for the overhead. pools are more made for bigger data
	// like 4KiB or more.
	bufPool sync.Pool // stores 64 bytes in each buf
}

func Init(dbPath string) (*metadataStore, error) {
	db, err := bolt.Open(dbPath, 0664, &bolt.Options{
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
