package main

import (
	"os"
	"text/tabwriter"

	"github.com/alecthomas/kong"
	"go.pabu.dev/packer"
	metadata "go.pabu.dev/packer/metadata/bolt"
)

type Context struct {
	ms packer.MetadataStore
}

var CLI struct {
	// flags
	DbPath string `env:"BOLT_DB" type:"path" required:"" help:"Path to bolt metadata db"`

	// commands
	Packs       PacksCmd        `cmd:"" help:"List packs"`
	PacksDelete PacksDeletedCmd `cmd:"" help:"List packs marked for deletion"`

	Chunks ChunksCmd `cmd:"" help:"List chunks"`
}

func main() {
	ctx := kong.Parse(&CLI)

	_, err := os.Stat(CLI.DbPath)
	ctx.FatalIfErrorf(err)

	ms := metadata.New(CLI.DbPath)
	err = ms.Init()
	ctx.FatalIfErrorf(err)

	err = ctx.Run(&Context{
		ms: ms,
	})

	if cerr := ms.Close(); cerr != nil {
		ctx.Errorf("closing packager: %v", cerr)
	}

	ctx.FatalIfErrorf(err)
}

func newTabs() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)
}
