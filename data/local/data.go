package local

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"sync/atomic"

	"github.com/gofrs/flock"
	"go.pabu.dev/packer"
	"go.pabu.dev/packer/data/local/internal/packread"
)

type dataStore struct {
	packDir string

	currentPackFile   *os.File
	currentPackID     uint64
	currentPackOffset atomic.Int64
	currentPackLock   *flock.Flock

	compactPackThreshold int64
	maxPackSize int64
	growBuf     bool

	packRead packread.PackRead
}

const minPackSize = 1024 << 10 // 1MiB
const defaultPackSize = 1024 << 16 // 64MiB
type Option func(ds *dataStore)

const prefixLen = 4

// if bytes is less than 1MiB, it will clamp to 1MiB
func WithMaxPackSize(bytes int64) Option {
	return func(ds *dataStore) {
		ds.maxPackSize = max(minPackSize, bytes)
	}
}

func WithCompactPackThreshold(bytes int64) Option {
	return func(ds *dataStore) {
		ds.compactPackThreshold = bytes
	}
}

// instead of returning and error when getting a chunk
// if the chunk won't fit in the buf, grow the buf.
func WithGrowBuf(b bool) Option {
	return func(ds *dataStore) {
		ds.growBuf = b
	}
}

// Default maxPackSize is 64MiB
func Init(packDir string, opts ...Option) (*dataStore, error) {

	if err := os.MkdirAll(packDir, 0775); err != nil {
		return nil, err
	}

	ds := dataStore{
		packDir:       packDir,
		packRead:      packread.New(),
		maxPackSize:   defaultPackSize,
	}

	for _, opt := range opts {
		opt(&ds)
	}

	if ds.compactPackThreshold <= 0 {
		ds.compactPackThreshold = ds.maxPackSize >> 1 + ds.maxPackSize >> 2
	}
	
	if _, err := ds.openPackWrite(); err != nil {
		return nil, err
	}

	return &ds, nil
}

// Returns error for closing write pack
func (ds *dataStore) Close() error {
	ds.packRead.Close()
	return ds.currentPackFile.Close()
}


func (ds *dataStore) Put(data []byte) (packer.ChunkInfo, error) {
	if len(data) > math.MaxUint32 {
		return packer.ChunkInfo{}, errors.New("data is larger than MaxUint32")
	}

	size64 := int64(len(data))
	sizeU32 := uint32(len(data))

	offset := ds.currentPackOffset.Add(size64) - size64

	if offset+size64+prefixLen >= ds.maxPackSize {
		newOffset, err := ds.openPackWriteNext()
		if err != nil {
			return packer.ChunkInfo{}, fmt.Errorf("opening new pack as it reached maxsize: %w", err)
		}
		offset = newOffset
	}

	sizeBytes := make([]byte, prefixLen)
	binary.LittleEndian.PutUint32(sizeBytes, sizeU32)

	// write size prefix
	if _, err := ds.currentPackFile.WriteAt(sizeBytes, offset); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing size: %w", err)
	}
	// write the data
	if _, err := ds.currentPackFile.WriteAt(data, offset+prefixLen); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing data: %w", err)
	}

	return packer.ChunkInfo{
		PackID:    ds.currentPackID,
		Offset:    uint64(offset - headerLen),
		SizeBytes: sizeU32,
	}, nil
}

// reads chunk into buf, returning the slice containing the chunk
//
// buf len needs to be equal or larger than ChunkInfo.SizeBytes + prefixLen
func (ds *dataStore) Get(ci packer.ChunkInfo, buf []byte) ([]byte, error) {

	expectedReadBytesI := int(ci.SizeBytes)+prefixLen
	if len(buf) <  expectedReadBytesI {
		if !ds.growBuf {
			return nil, errors.New("buf len is not big enough to contain the chunk")
		}
		needed := expectedReadBytesI - len(buf)
		buf = slices.Grow(buf, needed)[:expectedReadBytesI]
	}

	expectedReadBytes := ci.SizeBytes + prefixLen

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

	if size != ci.SizeBytes {
		return nil, fmt.Errorf("expected to size prefix to be %d, received %d", ci.SizeBytes, size)
	}

	data := buf[prefixLen:readBytes]

	return data, nil
}


func (ds *dataStore) NormalizePacks(packs map[uint64]packer.PackInfo) {
	for k, v := range packs {
		packs[k] = normalizePack(v)
	}
}

func (ds *dataStore) FilterCompactable(packs map[uint64]packer.PackInfo) {
	for packID, info := range packs {
		np := normalizePack(info)
		if !ds.shouldCompact(np) {
			delete(packs, packID)
		} else {
			packs[packID] = np
		}
	}
}

func (ds *dataStore) shouldCompact(pack packer.PackInfo) bool {
	return pack.ChunkTotalSize < uint64(ds.compactPackThreshold)
}

func normalizePack(pack packer.PackInfo) packer.PackInfo {
	pack.ChunkTotalSize += headerLen + (pack.ChunkCount * prefixLen)
	return pack
}
