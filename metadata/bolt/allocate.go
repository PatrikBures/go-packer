package metadata

import bolt "go.etcd.io/bbolt"

func (ms *metadataStore) AllocatePack() (uint64, error) {
	var packID uint64

	return packID, ms.db.Update(func(tx *bolt.Tx) error {
		bp := tx.Bucket(bucketPacks)
		if bp == nil {
			return ErrBucketNotFound
		}
		var err error
		packID, err = bp.NextSequence()

		return err
	})
}
