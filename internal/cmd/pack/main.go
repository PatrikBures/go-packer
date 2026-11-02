package main

import (
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	"go.pabu.dev/packer"
	"go.pabu.dev/packer/data/local"
	"go.pabu.dev/packer/metadata/bolt"
)

type Context struct {
	packager *packer.Packager
	tmpDir   string
}

var cli struct {
	Put       PutCmd       `cmd:"" help:"add files to packer"`
	Get       GetCmd       `cmd:"" help:"get data from packer"`
	Delete    DeleteCmd    `cmd:"" help:"delete chunk"`
	Chunks    ChunksCmd    `cmd:"" help:"get all chunks in packs"`
	PackInfo  PackInfoCmd  `cmd:"" help:"get chunk info about packs"`
	LsCompact LsCompactCmd `cmd:"" help:"list packs that would be compacted"`
	Compact   CompactCmd   `cmd:"" help:"compact packs"`
}

func main() {
	dir := filepath.Join(os.TempDir(), "testing-packer")

	p, err := setupPackager(dir)
	if err != nil {
		log.Fatal(err)
	}

	ctx := kong.Parse(&cli)

	err = ctx.Run(&Context{
		packager: p,
		tmpDir:   dir,
	})

	if cerr := p.Close(); cerr != nil {
		ctx.Errorf("closing packager: %v", cerr)
	}

	ctx.FatalIfErrorf(err)
}

func setupPackager(dir string) (*packer.Packager, error) {

	if err := os.Mkdir(dir, 0775); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	ms := metadata.New(filepath.Join(dir, "metadata.db"))
	
	if err := ms.Init(); err != nil {
		return nil, err
	}

	ds := data.New(dir, data.WithGrowBuf(true))
	if err := ds.Init(); err != nil {
		return nil, err
	}

	p := packer.New(ms, ds)

	return p, nil
}
