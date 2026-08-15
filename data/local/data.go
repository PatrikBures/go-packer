package local

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"

	"go.pabu.dev/packer"
	"go.pabu.dev/packer/data/local/internal/packread"
)

type dataStore struct {
	packDir           string
	currentPackFile   *os.File
	currentPackID     uint64
	currentPackOffset atomic.Uint64

	packRead packread.PackRead
}

func Init(packDir string, currentPackID uint64) (*dataStore, error) {
	stat, err := os.Stat(packDir)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("packdir is not a dir: %s", packDir)
	}

	ds := dataStore{
		packDir:       packDir,
		currentPackID: currentPackID,
		packRead:      packread.New(),
	}

	pack, err := ds.openPack(currentPackID, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}

	stat, err = pack.Stat()
	if err != nil {
		return nil, err
	}
	ds.currentPackOffset.Store(uint64(stat.Size()))

	ds.currentPackFile = pack

	return &ds, nil
}

func (ds *dataStore) Close() error {
	return errors.Join(ds.currentPackFile.Close(), ds.packRead.Close())
}

func (ds *dataStore) path(packID uint64) string {
	return filepath.Join(ds.packDir, fmt.Sprintf("%d.pack", packID))
}

func (ds *dataStore) openPack(packID uint64, mode int) (*os.File, error) {
	f, err := os.OpenFile(ds.path(packID), mode, 0664)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() == 0 {
		if err := initPack(packID, f); err != nil {
			return nil, fmt.Errorf("initializing pack: %w", err)
		}
	}

	return f, nil
}

func initPack(packID uint64, f *os.File) error {
	h := header{
		magic:   defaultHeaderMagic,
		version: 1,
		packID:  packID,
	}
	_, err := f.Write(h.Bytes())
	return err
}

func (ds *dataStore) Put(data []byte) (packer.ChunkInfo, error) {
	if len(data) > math.MaxUint32 {
		return packer.ChunkInfo{}, errors.New("data is larger than MaxUint32")
	}

	size64 := uint64(len(data))
	size32 := uint32(len(data))

	offset := ds.currentPackOffset.Add(size64) - size64

	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, size32)

	if _, err := ds.currentPackFile.WriteAt(sizeBytes, int64(offset)); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing size: %w", err)
	}
	if _, err := ds.currentPackFile.WriteAt(data, int64(offset)+4); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing data: %w", err)
	}

	return packer.ChunkInfo{
		PackId:    ds.currentPackID,
		Offset:    offset - headerLen,
		SizeBytes: size32,
	}, nil
}

// reads chunk into buf, returning the slice containing the chunk
//
// buf len needs to be equal or larger than ChunkInfo.SizeBytes + 4
func (ds *dataStore) Get(ci packer.ChunkInfo, buf []byte) ([]byte, error) {
	if len(buf) < int(ci.SizeBytes)+4 {
		return nil, errors.New("buf len is not big enough to contain the chunk")
	}

	var f *os.File
	if ci.PackId == ds.currentPackID {
		f = ds.currentPackFile
	} else if f = ds.packRead.Get(ci.PackId); f == nil {
		nf, err := ds.openPack(ci.PackId, os.O_RDONLY)
		if err != nil {
			return nil, fmt.Errorf("opening pack: %w", err)
		}
		f = nf
		ds.packRead.Put(ci.PackId, f)
	}

	readBytes, err := f.ReadAt(buf[:ci.SizeBytes+4], int64(headerLen)+int64(ci.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("reading pack: %w", err)
	}

	expectedReadBytes := 4 + ci.SizeBytes
	if readBytes != int(expectedReadBytes) {
		return nil, fmt.Errorf("expected to read %d bytes, read %d", expectedReadBytes, readBytes)
	}

	sizeBytes := buf[:4]
	size := binary.BigEndian.Uint32(sizeBytes)

	if size != ci.SizeBytes {
		return nil, fmt.Errorf("expected to size prefix to be %d, received %d", ci.SizeBytes, size)
	}

	data := buf[4:readBytes]

	return data, nil
}
