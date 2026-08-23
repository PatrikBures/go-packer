package local

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const headerLen = 128

var defaultHeaderMagic = [4]byte{'P', 'A', 'C', 'K'}

const majorVersion = 1
const minorVersion = 0

type header struct {
	Magic        [4]byte
	MajorVersion uint16
	MinorVersion uint16
	PackID       uint64
	Flags        headerFlags // 8 bytes
	State        headerState // 1 byte
	_            byte        // reserved for byte alignment
	HeaderSize   uint16
	Checksum     uint32
	_            [96]byte // reserved for future features
}

const (
	ho0Magic        = 0
	ho1MajorVersion = 4
	ho2MinorVersion = 6
	ho3PackID       = 8
	ho4Flags        = 16
	ho5State        = 24
	ho6Reserved     = 25
	ho7HeaderSize   = 26
	ho8Checksum     = 28
	ho9Reserved     = 32
)

type headerState uint8

const (
	stateCompleted  headerState = 0
	stateActive     headerState = 1
	stateCompacting headerState = 2
)

type headerFlags uint64

// example how in the future to implement flags
// const (
// 	FlagCompressed headerFlags = 1 << iota
// 	FlagEncrypted
// )

// checks if a specific or combination of flags are set
func (f headerFlags) Has(flag headerFlags) bool {
	return f&flag != 0
}
func (f *headerFlags) Set(flag headerFlags) {
	*f |= flag
}
func (f *headerFlags) Remove(flag headerFlags) {
	*f &^= flag
}
func (f *headerFlags) Toggle(flag headerFlags) {
	*f ^= flag
}

func (h *header) WriteTo(w io.Writer) (int64, error) {
	var data [headerLen]byte
	copy(data[ho0Magic:ho1MajorVersion], h.Magic[:])

	binary.LittleEndian.PutUint16(data[ho1MajorVersion:ho2MinorVersion], h.MajorVersion)
	binary.LittleEndian.PutUint16(data[ho2MinorVersion:ho3PackID], h.MinorVersion)
	binary.LittleEndian.PutUint64(data[ho3PackID:ho4Flags], h.PackID)
	binary.LittleEndian.PutUint64(data[ho4Flags:ho5State], uint64(h.Flags))
	data[ho5State] = byte(h.State)
	binary.LittleEndian.PutUint16(data[ho7HeaderSize:ho8Checksum], h.HeaderSize)
	binary.LittleEndian.PutUint32(data[ho8Checksum:ho9Reserved], 0) // checksum

	checksum := crc32.ChecksumIEEE(data[:])
	binary.LittleEndian.PutUint32(data[ho8Checksum:ho9Reserved], checksum)
	h.Checksum = checksum

	n, err := w.Write(data[:])
	return int64(n), err
}

func (h *header) ReadFrom(r io.Reader) (int64, error) {
	var data [headerLen]byte
	readBytes, err := io.ReadFull(r, data[:])
	if err != nil {
		return 0, err
	}
	if readBytes != headerLen {
		return 0, fmt.Errorf("reader did not have enough bytes for a header")
	}

	copy(h.Magic[:], data[ho0Magic:ho1MajorVersion])

	h.MajorVersion = binary.LittleEndian.Uint16(data[ho1MajorVersion:ho2MinorVersion])
	h.MinorVersion = binary.LittleEndian.Uint16(data[ho2MinorVersion:ho3PackID])
	h.PackID = binary.LittleEndian.Uint64(data[ho3PackID:ho4Flags])
	h.Flags = headerFlags(binary.LittleEndian.Uint64(data[ho4Flags:ho5State]))
	h.State = headerState(data[ho5State])
	h.HeaderSize = binary.LittleEndian.Uint16(data[ho7HeaderSize:ho8Checksum])
	checksum := binary.LittleEndian.Uint32(data[ho8Checksum:ho9Reserved])

	h.Checksum = 0
	binary.LittleEndian.PutUint32(data[ho8Checksum:ho9Reserved], 0)
	if crc32.ChecksumIEEE(data[:]) != checksum {
		return 0, ErrHeaderChecksum
	}
	h.Checksum = checksum

	if h.Magic != defaultHeaderMagic {
		return 0, fmt.Errorf("%w: recieved %x, expected: %x", ErrHeaderMagic, h.Magic, defaultHeaderMagic)
	}

	if h.MajorVersion != majorVersion {
		return 0, fmt.Errorf("%w: supports: %d, received: %d", ErrMajorVersionMissmatch, majorVersion, h.MajorVersion)
	}
	if h.MinorVersion > minorVersion {
		return 0, fmt.Errorf("%w: supports from %d, received: %d", ErrMinorVersion, minorVersion, h.MinorVersion)
	}

	return headerLen, nil
}
