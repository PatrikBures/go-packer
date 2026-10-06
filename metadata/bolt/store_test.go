package metadata

import (
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
	"go.pabu.dev/packer/metadata/metadatatest"
)

func TestSuite(t *testing.T) {
	metadatatest.RunSuite(t, initBoltP)
}


func initBoltP(t *testing.T) packer.MetadataStore {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "bolt.db")

	ms, err := Init(dbFile)
	if err != nil {
		t.Fatal(err)
	}

	return ms
}

func initBolt(t *testing.T) *metadataStore {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "bolt.db")

	ms, err := Init(dbFile)
	if err != nil {
		t.Fatal(err)
	}

	if ms.db == nil {
		t.Fatal("db is nil")
	}

	return ms
}

func TestInit(t *testing.T) {

	ms := initBolt(t)

	if err := ms.db.View(func(tx *bolt.Tx) error {
		version, err := ms.version()
		if err != nil {
			return err
		}
		t.Logf("schema version: %d", version)

		t.Log("Existing buckets:")
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			t.Logf("bucket name: %s", string(name))
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}

	packerMs := ms

	if err := packerMs.Close(); err != nil {
		t.Fatal(err)
	}
}
