package imptest

import (
	"fmt"
	"path/filepath"
	"testing"

	"go.pabu.dev/packer"
	localData "go.pabu.dev/packer/data/local"
	boltMetadata "go.pabu.dev/packer/metadata/bolt"
	"go.pabu.dev/packer/packertest"
)

func Fuzz_Bolt_Local(f *testing.F) {

	f.Add(uint64(123))

	f.Fuzz(func(t *testing.T, seed uint64) {
		packertest.RunRandomActions(t, packertest.RunRandomActionsConfig{
			Instances: 1, // bbolt supports only one read/write instance
			RoutinesPerInstance: 4,
			ActionQty: 100,
			Seed: seed,
			InitPackager: InitBoltAndLocalPackager,
		})
	})
}

func InitBoltAndLocalPackager(dir string, maxPackSize int) (*packer.Packager, error) {
	dbPath := filepath.Join(dir, "metadata.db")

	ms, err := boltMetadata.Init(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed initializing bolt metadataStore: %w", err)
	}

	ds, err := localData.Init(dir)
	if err != nil {
		return nil, fmt.Errorf("failed initializing local dataStore: %w", err)
	}

	return packer.New(ms, ds), nil
}

