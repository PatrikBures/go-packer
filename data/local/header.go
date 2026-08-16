package local

import (
	"encoding/binary"
	"errors"
	"slices"
)

const headerLen = 16

type header struct {
	magic   []byte
	version uint32
	packID  uint64
}

var defaultHeaderMagic = []byte("PACK")

func (h *header) Bytes() []byte {
	b := make([]byte, headerLen)

	copy(b[:4], h.magic)

	binary.LittleEndian.PutUint32(b[4:8], h.version)
	binary.LittleEndian.PutUint64(b[8:16], h.packID)

	return b
}

func (h *header) Equal(h2 header) bool {
	return !(!slices.Equal(h.magic, h2.magic) || h.packID != h2.packID || h.version != h2.version)
}

func toHeader(b []byte) (header, error) {
	if len(b) < headerLen {
		return header{}, errors.New("slice too small")
	}

	return header{
		magic:   b[:4],
		version: binary.LittleEndian.Uint32(b[4:8]),
		packID:  binary.LittleEndian.Uint64(b[8:16]),
	}, nil
}
