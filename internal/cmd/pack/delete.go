package main

import (
	"crypto/sha256"
	"fmt"
)

type DeleteCmd struct {
	Name string `arg:"" name:"name"`
}

func (c *DeleteCmd) Run(ctx *Context) error {

	hash := sha256.Sum256([]byte(c.Name))

	if err := ctx.packager.Delete(hash[:]); err != nil {
		return err
	}
	fmt.Printf("Deleted '%s'\n", c.Name)

	return nil
}
