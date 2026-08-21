package main

import (
	"fmt"
	"slices"

	"go.pabu.dev/bytesize"
	"go.pabu.dev/packer"
)


type PackInfoCmd struct {
	PackIDs []uint64 `arg:"" optional:"" name:"packIDs"`
}

func (c *PackInfoCmd) Run(ctx *Context) (err error) {

	var packInfo map[uint64]packer.PackInfo
	if c.PackIDs == nil {
		packInfo, err = ctx.packager.PackInfoAll()
	} else {
		packInfo, err = ctx.packager.PackInfo(c.PackIDs)
	}

	if err != nil {
		return err
	}

	if len(packInfo) == 0 {
		fmt.Println("no packs found")
		return nil
	}

	if c.PackIDs == nil {
		for k := range packInfo {
			c.PackIDs = append(c.PackIDs, k)
		}
	}

	slices.Sort(c.PackIDs)

	for _, packID := range c.PackIDs {
		info, ok := packInfo[packID]
		if !ok {
			continue
		}
		fmt.Printf("packID: % 4d, chunks: % 4d, totalSize: % 6s\n", packID, info.ChunkCount, bytesize.FormatIEC(info.ChunkTotalSize))
	}

	return nil
}
