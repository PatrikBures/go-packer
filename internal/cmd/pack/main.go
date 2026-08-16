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

type Context struct{
	packager *packer.Packager
	tmpDir string
}

var cli struct {
	Put PutCmd `cmd:"" help:"add files to packer"`
	Get GetCmd `cmd:"" help:"get data from packer"`
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
		tmpDir: dir,
	})

	ctx.FatalIfErrorf(err)
}


func setupPackager(dir string) (*packer.Packager, error) {

	if err := os.Mkdir(dir, 0775); err != nil && !errors.Is(err, os.ErrExist){
		return nil, err
	}

	ms, err := metadata.Init(filepath.Join(dir, "metadata.db"))
	if err != nil {
		return nil, err
	}

	ds, err := local.Init(dir, 1)
	if err != nil {
		return nil, err
	}

	p := packer.New(ms, ds)

	return p, nil
}
