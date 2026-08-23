package main

import (
	"fmt"
	"slices"
)

type LsCompactCmd struct {
	Human bool `name:"human" short:"u" help:"Human readable sizes"`
}

func (c *LsCompactCmd) Run(ctx *Context) error {

	packs, err := ctx.packager.WouldCompact()
	if err != nil {
		return err
	}

	if len(packs) == 0 {
		fmt.Println("no packs to compact")
		return nil
	}

	packIDs := make([]uint64, 0, len(packs))

	for packID := range packs {
		packIDs = append(packIDs, packID)
	}

	slices.Sort(packIDs)

	for _, packID := range packIDs {
		info, ok := packs[packID]
		if !ok {
			continue
		}

		printPackInfo(packID, info, c.Human)
	}

	return nil
}
