// bolt implementation of the MetadataStore interface
package metadata

import (
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

type metadataStore struct {
	db      *bolt.DB
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
	}, nil
}

func (ms *metadataStore) Close() error {
	return ms.db.Close()
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
