package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
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

	readBytes, err := f.ReadAt(buf[:expectedReadBytes], int64(ci.Offset))
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


func (ds *dataStore) GetReader(ci packer.ChunkInfo) (io.Reader, error) {

	f, err := ds.openPackRead(ci.PackID)
	if err != nil {
		return nil, fmt.Errorf("opening pack for reading: %w", err)
	}

	cr := &chunkReader{
		expectedTotalRead: int(ci.Size) + prefixLen,
		f: f,
		startOffset: int64(ci.Offset),
	}

	var prefix [prefixLen]byte
	if n, err := io.ReadFull(cr, prefix[:]); err != nil {
		return nil, fmt.Errorf("reading prefix from chunk: %w", err)
	} else if n != prefixLen {
		return nil, fmt.Errorf("did not read enough bytes for a prefix: %d", n)
	}

	if cr.totalRead != prefixLen {
		return nil, fmt.Errorf("total read is not prefixLen: %d", cr.totalRead)
	}
	
	size := binary.LittleEndian.Uint32(prefix[:4])
	if ci.Size != size {
		return nil, fmt.Errorf("expected size prefix to be %d, received %d", ci.Size, size)
	}

	return cr, nil
}

type chunkReader struct {
	f *os.File
	startOffset int64
	expectedTotalRead int
	totalRead int
}

func (cr *chunkReader) Read(p []byte) (int, error) {
	if cr.totalRead == cr.expectedTotalRead {
		return 0, io.EOF
	} else if cr.totalRead > cr.expectedTotalRead {
		return 0, fmt.Errorf("read more than chunk size: %d", cr.totalRead)
	}

	willRead := min(len(p), cr.expectedTotalRead - cr.totalRead)

	if n, err := cr.f.ReadAt(p[:willRead], cr.startOffset + int64(cr.totalRead)); err != nil {
		return n, fmt.Errorf("reading from the %dth byte from chunk at offset %d: %w", cr.totalRead, cr.startOffset + int64(cr.totalRead), err)
	} else if n == 0 {
		return n, fmt.Errorf("unexpectedy read 0 bytes from pack file")
	} else if n != willRead {
		return n, fmt.Errorf("did not read the expected amount, expected=%d, read=%d", willRead, n)
	}

	cr.totalRead += willRead

	return willRead, nil
}
