package metadata

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)


func (ms *metadataStore) Get(id []byte) (packer.ChunkInfo, error) {
	var ci packer.ChunkInfo

	if err := ms.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketChunks)
		if bucket == nil {
			return ErrBucketNotFound
		}

		v := bucket.Get(id)
		if v == nil {
			return fmt.Errorf("%w: metadata", packer.ErrChunkNotFound)
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
