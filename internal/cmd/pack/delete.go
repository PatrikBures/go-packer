package main

import (
	"crypto/sha256"
	"fmt"
)

type DeleteCmd struct {
	Name []string `arg:"" name:"names"`
}

func (c *DeleteCmd) Run(ctx *Context) error {

	for _, n := range c.Name {
		hash := sha256.Sum256([]byte(n))

		if err := ctx.packager.Delete(hash[:]); err != nil {
			return err
		}
		fmt.Printf("Deleted '%s'\n", n)
	}

	return nil
}
