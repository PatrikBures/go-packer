package main

import (
	"fmt"

	"go.pabu.dev/packer"
)

type CompactCmd struct{}

func (c *CompactCmd) Run(ctx *Context) error {
	chunks, err := ctx.packager.CompactablePackChunks()
	if err != nil {
		return err
	}
	fmt.Printf("can compact from %d chunks\n", len(chunks))

	var compInfoAll packer.CompactInfo
	for {
		newChunks, compInfo, err := ctx.packager.CompactNext(chunks)
		if err != nil {
			return err
		}
		chunks = newChunks
		if compInfo.PacksCreated == 0 {
			break
		}
		compInfoAll.Merge(compInfo)

		fmt.Printf("%s\n", compInfo)
	}

	fmt.Println("Deleting unreferenced packs...")

	duInfo, err := ctx.packager.DeleteUnreferencedPacks()
	if err != nil {
		return err
	}
	compInfoAll.Merge(duInfo)

	fmt.Printf("%s\n", compInfoAll)
	return nil
}
