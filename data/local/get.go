package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"

	"go.pabu.dev/packer"
)

// Reads chunk into buf, returning the slice containing the chunk
//
// If !growBuf, then buf len needs to be equal or larger than ChunkInfo.Size + prefixLen
func (ds *dataStore) Get(ci packer.ChunkInfo, buf []byte) ([]byte, error) {

	expectedReadBytesI := int(ci.Size) + prefixLen
	if len(buf) < expectedReadBytesI {
		if !ds.growBuf {
			return nil, errors.New("buf len is not big enough to contain the chunk")
		}
		needed := expectedReadBytesI - len(buf)
		buf = slices.Grow(buf, needed)[:expectedReadBytesI]
	}

	expectedReadBytes := ci.Size + prefixLen

	f, err := ds.openPackRead(ci.PackID)
	if err != nil {
		return nil, fmt.Errorf("opening pack for reading: %w", err)
	}

	readBytes, err := f.ReadAt(buf[:expectedReadBytes], int64(headerLen)+int64(ci.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("reading pack: %w", err)
	}

	if readBytes != int(expectedReadBytes) {
		return nil, fmt.Errorf("expected to read %d bytes, read %d", expectedReadBytes, readBytes)
	}

	sizeBytes := buf[:4]
	size := binary.LittleEndian.Uint32(sizeBytes)

	if size != ci.Size {
		return nil, fmt.Errorf("packID: %d, offset: %d, expected size prefix to be %d, received %d", ci.PackID, ci.Offset, ci.Size, size)
	}

	data := buf[prefixLen:readBytes]

	return data, nil
}
