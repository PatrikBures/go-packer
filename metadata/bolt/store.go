// bolt implementation of the MetadataStore interface
package metadata

import (
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

type metadataStore struct {
	dbPath  string
	db      *bolt.DB
}

func New(dbPath string) *metadataStore {
	return &metadataStore{
		dbPath: dbPath,
	}
}

func (ms *metadataStore) Init() error {
	db, err := bolt.Open(ms.dbPath, 0664, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return err
	}

	if err := migrateToVersion(db, 0); err != nil {
		return fmt.Errorf("migrating db to latest version: %w", err)
	}

	ms.db = db

	return nil
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
