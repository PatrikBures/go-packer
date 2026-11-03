package main

import (
	"cmp"
	"fmt"
	"slices"

	"go.pabu.dev/bytesize"
	"go.pabu.dev/packer"
)

type PacksCmd struct {
	Human bool `name:"human" short:"H" help:"Human readable sizes"`
}

func (cmd *PacksCmd) Run(ctx *Context) error {
	packs, err := ctx.ms.PackInfoAll()
	if err != nil {
		return err
	}

	w := newTabs()

	if _, err := fmt.Fprintf(w, "ID\tCOUNT\tSIZE\tSTATE\n"); err != nil {
		return err
	}

	packsSlice := make([]packer.PackInfoWithID, 0, len(packs))
	for id, info := range packs {
		packsSlice = append(packsSlice, packer.PackInfoWithID{
			Id:       id,
			PackInfo: info,
		})
	}

	slices.SortFunc(packsSlice, func(a, b packer.PackInfoWithID) int {
		return cmp.Compare(a.Id, b.Id)
	})

	var pfn func(info packer.PackInfoWithID) error
	if cmd.Human {
		pfn = func(info packer.PackInfoWithID) error {
			_, err := fmt.Fprintf(w, "%d\t%d\t%s\t%d\n", info.Id, info.ChunkCount, bytesize.FormatSI(info.Size), info.State)
			return err
		}
	} else {
		pfn = func(info packer.PackInfoWithID) error {
			_, err := fmt.Fprintf(w, "%d\t%d\t%d\t%d\n", info.Id, info.ChunkCount, info.Size, info.State)
			return err
		}
	}
	for _, info := range packsSlice {
		if err := pfn(info); err != nil {
			return err
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("found %d packs\n", len(packs))

	return nil
}
