package main

import (
	"fmt"

	"go.pabu.dev/packer"
)

type CompactCmd struct{}

func (c *CompactCmd) Run(ctx *Context) error {
	packChunks, err := ctx.packager.CompactablePacks()
	if err != nil {
		return err
	}
	for packID, chunks := range packChunks {
		fmt.Printf("packID: % 4d, chunks: % 4d\n", packID, len(chunks))
	}
	fmt.Printf("can compact from %d packs\n", len(packChunks))

	var compInfoAll packer.CompactInfo
	for {
		compInfo, err := ctx.packager.CompactNext(packChunks)
		if err != nil {
			return err
		}
		if compInfo.PacksCreated == 0 {
			break
		}
		compInfoAll.Merge(compInfo)

		fmt.Printf("%+v\n", compInfo)
	}

	fmt.Printf("%+v\n", compInfoAll)
	return nil
}
