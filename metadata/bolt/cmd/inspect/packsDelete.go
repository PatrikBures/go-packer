package main

import (
	"fmt"
)

type PacksDeletedCmd struct {}

func (cmd *PacksDeletedCmd) Run(ctx *Context) error {
	ids, err := ctx.ms.PacksDelete()
	if err != nil {
		return err
	}

	for _, id := range ids {
		fmt.Println(id)
	}

	fmt.Printf("found %d packs to delete\n", len(ids))

	return nil
}
