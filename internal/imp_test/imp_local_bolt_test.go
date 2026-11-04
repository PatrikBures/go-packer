package imptest

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.pabu.dev/packer"
	localData "go.pabu.dev/packer/data/local"
	boltMetadata "go.pabu.dev/packer/metadata/bolt"
	"go.pabu.dev/packer/packertest"
)

func TestLong_Bolt_Local(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long")
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("Seed: %d", seed)

	packertest.RunRandomActions(t, packertest.RunRandomActionsConfig{
		Instances:           1, // bbolt supports only one read/write instance
		RoutinesPerInstance: 1,
		ActionQty:           1000_000_0,
		Seed:                seed,
		InitPackager:        InitBoltAndLocalPackager,
	})
}

func Test_Bolt_Local(t *testing.T) {
	packertest.RunSuite(t, InitPackagerT(t, InitBoltAndLocalPackager))
}

func InitBoltAndLocalPackager(dir string, maxPackSize int) (*packer.Packager, error) {
	dbPath := filepath.Join(dir, "metadata.db")

	ms := boltMetadata.New(dbPath)
	ds := localData.New(dir,
		localData.WithMaxPackSize(int64(maxPackSize)),
	)

	p := packer.New(ms, ds)
	if err := p.Init(); err != nil {
		return nil, fmt.Errorf("failed initializing local dataStore: %w", err)
	}

	return packer.New(ms, ds), nil
}

func InitPackagerT(t *testing.T, initPackager func(dir string, maxPackSize int) (*packer.Packager, error)) packertest.StoreFactory {
	return func(t *testing.T) *packer.Packager {
		dir := t.TempDir()

		p, err := initPackager(dir, 0)
		if err != nil {
			t.Fatal(err)
		}

		return p
	}
}
