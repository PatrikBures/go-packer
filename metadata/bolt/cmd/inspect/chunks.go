package main

import (
	"cmp"
	"fmt"
	"slices"

	"go.pabu.dev/bytesize"
	"go.pabu.dev/packer"
)

type ChunksCmd struct {
	PackIDs []uint64 `name:"packs" help:"Limit chunks to specific packs"`
	Human   bool     `name:"human" short:"H" help:"Human readable sizes"`
}

func (cmd *ChunksCmd) Run(ctx *Context) error {
	var chunks []packer.ChunkInfo
	if cmd.PackIDs != nil {
		var err error
		chunks, err = ctx.ms.Chunks(cmd.PackIDs)
		if err != nil {
			return err
		}
	} else {
		var err error
		chunks, err = ctx.ms.ChunksAll()
		if err != nil {
			return err
		}
	}

	slices.SortFunc(chunks, func(a, b packer.ChunkInfo) int {
		c := cmp.Compare(a.PackID, b.PackID)
		if c == 0 {
			return cmp.Compare(a.Offset, b.Offset)
		}
		return c
	})

	w := newTabs()

	if _, err := fmt.Fprintf(w, "PACKID\tOFFSET\tSIZE\n"); err != nil {
		return err
	}

	var pfn func(chunk packer.ChunkInfo) error
	if cmd.Human {
		pfn = func(chunk packer.ChunkInfo) error {
			_, err := fmt.Fprintf(w, "%d\t%s\t%s\n", chunk.PackID, bytesize.FormatSI(chunk.Offset), bytesize.FormatSI(chunk.Size))
			return err
		}
	} else {
		pfn = func(chunk packer.ChunkInfo) error {
			_, err := fmt.Fprintf(w, "%d\t%d\t%d\n", chunk.PackID, chunk.Offset, chunk.Size)
			return err
		}
	}
	for _, chunk := range chunks {
		if err := pfn(chunk); err != nil {
			return err
		}
	}

	w.Flush()

	fmt.Printf("found %d chunks\n", len(chunks))

	return nil
}
