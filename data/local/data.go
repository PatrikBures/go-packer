package local

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
	"go.pabu.dev/packer"
	"go.pabu.dev/packer/data/local/internal/packread"
)

type dataStore struct {
	packDir           string

	currentPackFile   *os.File
	currentPackID     uint64
	currentPackOffset atomic.Int64
	currentPackLock   *flock.Flock

	maxPackSize int64

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
	ds.currentPackOffset.Store(stat.Size())

	ds.currentPackFile = pack

	return &ds, nil
}

func (ds *dataStore) Close() error {
	return errors.Join(ds.currentPackFile.Close(), ds.packRead.Close())
}



func (ds *dataStore) openPack(packID uint64, mode int) (*os.File, error) {
	f, err := os.OpenFile(ds.completedPackPath(packID), mode, 0664)
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


func (ds *dataStore) newPack() error {
	newPackID := ds.currentPackID + 1

	f, err := ds.openPack(newPackID, os.O_CREATE|os.O_RDWR|os.O_EXCL)
	if err != nil {
		return err
	}

	ds.currentPackID = newPackID
	ds.currentPackFile = f
	ds.currentPackOffset.Store(headerLen)

	return nil
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

	size64 := int64(len(data))
	sizeU32 := uint32(len(data))

	offset := ds.currentPackOffset.Add(size64) - size64

	if offset + size64 + 4 >= ds.maxPackSize {
		if err := ds.newPack(); errors.Is(err, os.ErrExist) {
			// if the new pack already exists it means a different
			// instance/goroutine already created it.
			// so we wait a bit to make sure it is initialized.
			time.Sleep(time.Millisecond*20) 
		} else if err != nil {
			return packer.ChunkInfo{}, err
		}
		offset = ds.currentPackOffset.Add(size64) - size64 // new offset because file changed
	}

	sizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBytes, sizeU32)

	// write size prefix
	if _, err := ds.currentPackFile.WriteAt(sizeBytes, offset); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing size: %w", err)
	}
	// write the data
	if _, err := ds.currentPackFile.WriteAt(data, offset + 4); err != nil {
		return packer.ChunkInfo{}, fmt.Errorf("writing data: %w", err)
	}

	return packer.ChunkInfo{
		PackId:    ds.currentPackID,
		Offset:    uint64(offset - headerLen),
		SizeBytes: sizeU32,
	}, nil
}

// reads chunk into buf, returning the slice containing the chunk
//
// buf len needs to be equal or larger than ChunkInfo.SizeBytes + 4
func (ds *dataStore) Get(ci packer.ChunkInfo, buf []byte) ([]byte, error) {
	if len(buf) < int(ci.SizeBytes)+4 {
		return nil, errors.New("buf len is not big enough to contain the chunk")
	}
	expectedReadBytes := 4 + ci.SizeBytes

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

	data := buf[4:readBytes]

	return data, nil
}
