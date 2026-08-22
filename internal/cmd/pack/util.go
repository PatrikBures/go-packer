package main

import (
	"fmt"

	"go.pabu.dev/bytesize"
	"go.pabu.dev/packer"
)

func printPackInfo(packID uint64, pack packer.PackInfo, human bool) {

	var ts string
	if human {
		ts = fmt.Sprintf("% 6s", bytesize.FormatIEC(pack.ChunkTotalSize))
	} else {
		ts = fmt.Sprintf("% 10d", pack.ChunkTotalSize)
	}

	fmt.Printf("packID: % 4d, chunks: % 4d, totalSize: %s\n", packID, pack.ChunkCount, ts)
}
