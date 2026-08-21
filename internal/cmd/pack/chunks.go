package main

import "fmt"


type ChunksCmd struct {
	PackIDs []uint64 `arg:"" name:"packIDs"`
}

func (c *ChunksCmd) Run(ctx *Context) error {

	chunks, err := ctx.packager.Chunks(c.PackIDs)
	if err != nil {
		return err
	}

	if len(chunks) == 0 {
		fmt.Println("no chunks found")
		return nil
	}

	for _, c := range chunks {
		fmt.Printf("%+v\n", c)
	}

	return nil
}
