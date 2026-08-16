package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

type PutCmd struct {
	Name string `arg:"" name:"name"`
	File *os.File `arg:"" name:"existingfile" type:"file" help:"path to put into packer"`
}

func (c *PutCmd) Run(ctx *Context) error {

	data, err := io.ReadAll(c.File)
	if err != nil {
		return err
	}

	hash := sha256.Sum256([]byte(c.Name))
	dataHash := sha256.Sum256(data)

	if err := ctx.packager.Put(hash[:], data); err != nil {
		return err
	}

	fmt.Printf("added '%s'\n", c.Name)
	fmt.Printf("datahash: %s\n", hex.EncodeToString(dataHash[:]))

	return nil
}

