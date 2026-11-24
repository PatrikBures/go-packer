package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

type PutCmd struct {
	Name  string `arg:"" name:"name"`
	Data  []byte `arg:"" name:"existingfile" type:"filecontent" help:"File to pack. Use '-' to read stdin"`
	Count uint   `name:"count" default:"1" help:"Put same chunk multiple times"`
}

func (c *PutCmd) Run(ctx *Context) error {

	dataHash := sha256.Sum256(c.Data)

	if c.Count == 1 {
		pctx, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*500))
		hash := sha256.Sum256([]byte(c.Name))
		if err := ctx.packager.Put(pctx, hash[:], c.Data); err != nil {
			return err
		}

		fmt.Printf("added '%s'\n", c.Name)
		fmt.Printf("datahash: %s\n", hex.EncodeToString(dataHash[:]))

		return nil
	}
	for i := range c.Count {
		pctx, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*500))
		name := fmt.Sprintf("%s%d", c.Name, i+1)
		hash := sha256.Sum256([]byte(name))
		if err := ctx.packager.Put(pctx, hash[:], c.Data); err != nil {
			return err
		}

		fmt.Printf("added '%s'\n", name)
		fmt.Printf("datahash: %s\n", hex.EncodeToString(dataHash[:]))
	}

	return nil
}
