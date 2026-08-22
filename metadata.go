package packer

import (
	"encoding/binary"
	"errors"
)

type MetadataStore interface {
	Close() error
	Put(id []byte, ci ChunkInfo) error
	Get(id []byte) (ChunkInfo, error)
	Exists(id []byte) (bool, error)
	Delete(id []byte) error
	Chunks(packIDs []uint64) ([]ChunkInfo, error)
	PackInfo(packIDs []uint64) (map[uint64]PackInfo, error)
	PackInfoAll() (map[uint64]PackInfo, error)
}

const ChunkInfoSize = 20

type ChunkInfo struct {
	PackID    uint64 // [0:8]
	Offset    uint64 // [8:16]
	SizeBytes uint32 // [16:20]
}

type PackInfo struct {
	ChunkCount     uint64
	ChunkTotalSize uint64
}

func (ci *ChunkInfo) Bytes() []byte {
	buf := make([]byte, ChunkInfoSize)
	ci.PutBytes(buf)
	return buf
}

// converts ChunkInfo into buf[0:20]
//
// panics if buf is len is less than 20
func (ci *ChunkInfo) PutBytes(buf []byte) {
	_ = buf[ChunkInfoSize-1] // early bound check
	binary.LittleEndian.PutUint64(buf, ci.PackID)
	binary.LittleEndian.PutUint64(buf[8:16], ci.Offset)
	binary.LittleEndian.PutUint32(buf[16:20], ci.SizeBytes)
}

var (
	ErrSliceTooSmall = errors.New("provided slice is too small")
)

// can return ErrSliceTooSmall
func BytesToChunkInfo(b []byte) (ChunkInfo, error) {
	if len(b) < ChunkInfoSize {
		return ChunkInfo{}, ErrSliceTooSmall
	}
	return ChunkInfo{
		PackID:    binary.LittleEndian.Uint64(b[0:8]),
		Offset:    binary.LittleEndian.Uint64(b[8:16]),
		SizeBytes: binary.LittleEndian.Uint32(b[16:20]),
	}, nil
}
