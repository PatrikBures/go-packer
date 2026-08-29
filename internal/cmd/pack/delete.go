package main

import (
	"crypto/sha256"
	"fmt"
)

type DeleteCmd struct {
	Names []string `arg:"" name:"names"`
}

func (c *DeleteCmd) Run(ctx *Context) error {

	hashes := make([][]byte, 0, len(c.Names))
	for _, n := range c.Names {
		hash := sha256.Sum256([]byte(n))
		hashes = append(hashes, hash[:])
	}

	if err := ctx.packager.Delete(hashes); err != nil {
		return err
	}

	fmt.Printf("Deleted %d chunks\n", len(hashes))

	return nil
}
