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
	"sync"

	"github.com/gofrs/flock"
	"go.pabu.dev/packer"
	"go.pabu.dev/packer/data/local/internal/packread"
)

type dataStore struct {
	packDir string

	currentPackFile   *os.File
	currentPackLock   *flock.Flock
	currentPackOffset int64
	currentPackID     uint64
	nextPackID        uint64 // the next pack that should be attempted to lock

	writePackMu       sync.Mutex

	compactPackRatio float64
	maxPackSize          int64
	growBuf              bool

	packRead packread.PackRead
}

const minPackSize = 1024 << 10     // 1MiB
const defaultPackSize = 1024 << 16 // 64MiB
type Option func(ds *dataStore)

const prefixLen = 4

// If bytes is less than 1MiB, it will clamp to 1MiB.
// Default is 64MiB.
func WithMaxPackSize(bytes int64) Option {
	return func(ds *dataStore) {
		ds.maxPackSize = max(minPackSize, bytes)
	}
}

// Ratio of existing chunks in a pack compared with the total size of the pack.
//
// If the ratio is below 0 it will be set to 0.75, which is the default value.
// Ratio is clamped to max 1.
func WithCompactPackThreshold(ratio float64) Option {
	return func(ds *dataStore) {
		ds.compactPackRatio = max(min(ratio, 1), -1)
	}
}

// instead of returning and error when getting a chunk
// if the chunk won't fit in the buf, grow the buf.
// it should mainly be used if the size limit of chunks is not known.
func WithGrowBuf(b bool) Option {
	return func(ds *dataStore) {
		ds.growBuf = b
	}
}

// Creates packDir, initializes database and prepares pack for writing.
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

	if ds.compactPackRatio < 0 {
		ds.compactPackRatio = 0.75
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

	// using defer mutex does not allow multiple writes to same pack.
	// in the futurure, it could allocate the required data size, and then write. 
	// that should allow multiple writes to same pack.
	//
	// the issue is with having just one chunk allowed to be written to a pack,
	// is that if one user has slow upload speed, it will slow down other users 
	// writing to the same pack. 
	//
	// ideally it would unlock once it allocated the chunk in the pack.
	// or it could pre-allocate the entire pack with maxPackSize, and then trim it 
	// once the pack becomes full and is moved to completed.

	ds.writePackMu.Lock()
	defer ds.writePackMu.Unlock()

	totalSize := int64(len(data) + prefixLen)

	if ds.currentPackOffset+totalSize >= ds.maxPackSize {
		if _, err := ds.openPackWriteNext();  err != nil {
			return packer.ChunkInfo{}, fmt.Errorf("opening new pack as it reached maxsize: %w", err)
		}
	}

	offset := ds.currentPackOffset
	
	ci, err := putChunk(ds.currentPackID, data, ds.currentPackFile, offset)
	if err != nil {
		return packer.ChunkInfo{}, err
	}

	ds.currentPackOffset += totalSize

	return ci, nil
}

// Writes data starting at offset to file. 
// Prepends the prefix and its data before the chunk.
// Removes headerLen from the final offset (not sure if to keep).
// 
// packID is just used for the returning ChunkInfo.
func putChunk(packID uint64, data []byte, f *os.File, offset int64) (packer.ChunkInfo, error) {
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

	if err := f.Sync(); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("syncing chunk: %w", err)
	}

	return packer.ChunkInfo{
		PackID:    packID,
		Offset:    uint64(offset - headerLen),
		SizeBytes: sizeU32,
	}, nil
}

// Reads chunk into buf, returning the slice containing the chunk
//
// If !growBuf, then buf len needs to be equal or larger than ChunkInfo.SizeBytes + prefixLen
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

// Ignores deletion of any active packs.
// Removes pack from read cache.
func (ds *dataStore) DeletePacks(packs []uint64) (bytesRemoved int64, err error) {
	for _, packID := range packs {

		var stat os.FileInfo

		// stats file, continuing if it exists in active
		stat, err = os.Stat(ds.completedPackPath(packID))
		if errors.Is(err, os.ErrNotExist) {
			// this still causes the next err check to error if active does not exist
			if _, aerr := os.Stat(ds.activePackPath(packID)); aerr == nil {
				continue
			}
		}
		if err != nil {
			return bytesRemoved, fmt.Errorf("stating pack: %w", err)
		}

		_ = ds.packRead.ClosePack(packID)

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


// Returns a batch of packs
func (ds *dataStore) PacksBatch(startPackID uint64, packs []uint64) ([]uint64, error) {
	startLen := len(packs)

	dirID := ds.completedPackDirID(startPackID)
	dirIDLimit := dirID + 1000

	for ; startLen == len(packs) && dirID < dirIDLimit; dirID++ {
		dir := ds.completedPackDir(dirID)

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
			packs = append(packs, packID)
			return nil
		}); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrExist) && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("removing remaining empty packDir: %w", err)
		}
	}

	slices.Sort(packs)

	return packs, nil
}
