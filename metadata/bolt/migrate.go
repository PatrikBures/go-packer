package metadata

import (
	"encoding/binary"

	bolt "go.etcd.io/bbolt"
)

// if targetVersion < 1, it will migrate to the latest version
// if targetVersion > latestVersion it will clamp it to the latest version
func migrateToVersion(db *bolt.DB, targetVersion uint64) error {
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketVersion)
		return err
	}); err != nil {
		return err
	}

	var currentVersion uint64
	db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketVersion)
		if bucket == nil {
			return ErrBucketNotFound
		}
		v := bucket.Get(keyVersion)
		if v != nil {
			currentVersion = binary.LittleEndian.Uint64(v)
		}
		return nil
	})

	if currentVersion == uint64(len(migrations)) {
		return nil
	}

	if targetVersion < 1 {
		targetVersion = uint64(len(migrations))
	} else if targetVersion > uint64(len(migrations)) {
		targetVersion = uint64(len(migrations))
	}

	var i uint64
	if currentVersion != 0 {
		i = currentVersion - 1
	}
	for ; i < targetVersion; i++ {
		m := migrations[i]

		if err := db.Update(func(tx *bolt.Tx) error {
			versionBucket := tx.Bucket(bucketVersion)
			if versionBucket == nil {
				return ErrBucketNotFound
			}

			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, i+1)
			versionBucket.Put(keyVersion, buf)

			return m(tx)
		}); err != nil {
			return err
		}
	}

	return nil
}

var bucketChunks = []byte("chunks")
var bucketVersion = []byte("version")

var keyVersion = []byte("current")

var migrations = []func(tx *bolt.Tx) error{
	func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucketChunks)
		return err
	},
	func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("packs"))
		return err
	},
}
