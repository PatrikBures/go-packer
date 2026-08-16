package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type PutCmd struct {
	Name string `arg:"" name:"name"`
	Data []byte `arg:"" name:"existingfile" type:"filecontent" help:"File to pack. Use '-' to read stdin"`
}

func (c *PutCmd) Run(ctx *Context) error {


	hash := sha256.Sum256([]byte(c.Name))
	dataHash := sha256.Sum256(c.Data)

	if err := ctx.packager.Put(hash[:], c.Data); err != nil {
		return err
	}

	fmt.Printf("added '%s'\n", c.Name)
	fmt.Printf("datahash: %s\n", hex.EncodeToString(dataHash[:]))

	return nil
}

