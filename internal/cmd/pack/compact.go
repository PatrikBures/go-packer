package main

import (
	"fmt"
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

	for {
		compInfo, err := ctx.packager.CompactNext(packChunks)
		if err != nil {
			return err
		}
		if compInfo.PacksCreated == 0 {
			break
		}
		fmt.Printf("%+v\n", compInfo)
	}

	return nil
}
