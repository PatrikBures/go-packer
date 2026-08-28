package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/gofrs/flock"
	"go.pabu.dev/packer"
	"go.pabu.dev/packer/data/local/internal/packread"
)

type dataStore struct {
	packDir string

	currentPackFile   *os.File
	currentPackOffset atomic.Int64
	currentPackLock   *flock.Flock
	currentPackID     uint64
	nextPackID        atomic.Uint64 // the next pack that should be attempted to lock

	compactPackThreshold int64
	maxPackSize          int64
	growBuf              bool

	packRead packread.PackRead
}

const minPackSize = 1024 << 10     // 1MiB
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
		packDir:     packDir,
		packRead:    packread.New(),
		maxPackSize: defaultPackSize,
	}

	for _, opt := range opts {
		opt(&ds)
	}

	if ds.compactPackThreshold <= 0 {
		ds.compactPackThreshold = ds.maxPackSize>>1 + ds.maxPackSize>>2
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

	offset := ds.currentPackOffset.Add(size64) - size64

	if offset+size64+prefixLen >= ds.maxPackSize {
		newOffset, err := ds.openPackWriteNext()
		if err != nil {
			return packer.ChunkInfo{}, fmt.Errorf("opening new pack as it reached maxsize: %w", err)
		}
		offset = newOffset
	}

	return ds.put(ds.currentPackID, data, ds.currentPackFile, offset)
}

func (ds *dataStore) put(packID uint64, data []byte, f *os.File, offset int64) (packer.ChunkInfo, error) {
	if len(data) > math.MaxUint32 {
		return packer.ChunkInfo{}, errors.New("data is larger than MaxUint32")
	}

	sizeU32 := uint32(len(data))

	var prefixBytes [prefixLen]byte
	binary.LittleEndian.PutUint32(prefixBytes[:], sizeU32)

	// write size prefix
	if _, err := f.WriteAt(prefixBytes[:], offset); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing size: %w", err)
	}
	// write data
	if _, err := f.WriteAt(data, offset+prefixLen); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing data: %w", err)
	}

	return packer.ChunkInfo{
		PackID:    packID,
		Offset:    uint64(offset - headerLen),
		SizeBytes: sizeU32,
	}, nil
}

// reads chunk into buf, returning the slice containing the chunk
//
// if !growBuf, then buf len needs to be equal or larger than ChunkInfo.SizeBytes + prefixLen
func (ds *dataStore) Get(ci packer.ChunkInfo, buf []byte) ([]byte, error) {

	expectedReadBytesI := int(ci.SizeBytes) + prefixLen
	if len(buf) < expectedReadBytesI {
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
		return nil, fmt.Errorf("packID: %d, offset: %d, expected size prefix to be %d, received %d", ci.PackID, ci.Offset, ci.SizeBytes, size)
	}

	data := buf[prefixLen:readBytes]

	return data, nil
}

// Ignores deletion of any active packs
func (ds *dataStore) DeletePacks(packs []uint64) (bytesRemoved int64, err error) {
	for _, packID := range packs {

		var stat os.FileInfo

		if f := ds.packRead.GetRemovePack(packID); f != nil {
			// removes pack from cache and getting its stat

			stat, err = f.Stat()
			if err != nil {
				return bytesRemoved, fmt.Errorf("stating cached pack: %w", err)
			}

			_ = f.Close()
		} else {
			// stats file, continuing if it exists in active

			stat, err = os.Stat(ds.completedPackPath(packID))
			if errors.Is(err, os.ErrNotExist) {
				if _, aerr := os.Stat(ds.activePackPath(packID)); aerr == nil {
					continue
				}
			}
			if err != nil {
				return bytesRemoved, fmt.Errorf("stating pack: %w", err)
			}
		}
		size := stat.Size()

		err = os.Remove(ds.completedPackPath(packID))
		if errors.Is(err, os.ErrNotExist) {
			if _, aerr := os.Stat(ds.activePackPath(packID)); aerr == nil {
				continue
			}
		}	
		if err != nil {
			return bytesRemoved, fmt.Errorf("removing file pack: %w", err)
		}

		bytesRemoved += size
	}
	return bytesRemoved, nil
}


func (ds *dataStore) PacksBatch(startPackID uint64, packs []uint64) ([]uint64, error) {
	dir := filepath.Dir(ds.completedPackPath(startPackID))

	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		packID, err := packIdFromPath(path)
		if err != nil {
			return err
		}
		fmt.Println("found completed pack: ", packID)
		packs = append(packs, packID)
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	slices.Sort(packs)

	return packs, nil
}
