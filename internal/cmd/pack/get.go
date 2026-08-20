package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type GetCmd struct {
	Name string `arg:"" name:"name"`
}

func (c *GetCmd) Run(ctx *Context) error {

	hash := sha256.Sum256([]byte(c.Name))

	data, err := ctx.packager.Get(hash[:])
	if err != nil {
		return err
	}

	datahash := sha256.Sum256(data)
	fmt.Println("datahash:", hex.EncodeToString(datahash[:]))
	if len(data) < 1024 {
		fmt.Println("data:")
		fmt.Println(string(data))
	}

	return nil
}
