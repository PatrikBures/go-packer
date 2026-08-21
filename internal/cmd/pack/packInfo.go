package main

import (
	"fmt"

	"go.pabu.dev/bytesize"
)


type PackInfoCmd struct {
	PackIDs []uint64 `arg:"" name:"packIDs"`
}

func (c *PackInfoCmd) Run(ctx *Context) error {

	packInfo, err := ctx.packager.PackInfo(c.PackIDs)
	if err != nil {
		return err
	}

	if len(packInfo) == 0 {
		fmt.Println("no packs found")
		return nil
	}

	for _, packID := range c.PackIDs {
		info, ok := packInfo[packID]
		if !ok {
			continue
		}
		fmt.Printf("packID: % 4d, chunks: % 4d, totalSize: % 6s\n", packID, info.ChunkCount, bytesize.FormatIEC(info.ChunkTotalSize))
	}

	return nil
}
