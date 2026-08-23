package local

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestHeaderBytes(t *testing.T) {
	testData := []struct{
		in header
		writeErr error
		readErr error
	} {
		{
			in: header{
				Magic:   [4]byte{'P', 'A', 'C', 'K'},
				MajorVersion: 1,
				MinorVersion: 0,
				PackID:  89,
				State: stateCompacting,
				HeaderSize: 128,
			},
			writeErr: nil,
			readErr: nil,
		},
		{
			in: header{
				Magic: [4]byte{'A', 's', 'd', 'f'},
			},
			readErr: ErrHeaderMagic,
		},
		{
			in: header{
				Magic:   defaultHeaderMagic,
				MajorVersion: 8932,
				MinorVersion: 0,
			},
			readErr: ErrMajorVersionMissmatch,
		},
		{
			in: header{
				Magic:   defaultHeaderMagic,
				MajorVersion: 1,
				MinorVersion: 10,
			},
			readErr: ErrMinorVersion,
		},
	}

	buf := bytes.NewBuffer(make([]byte, 0, 128))
	for i, td := range testData {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			_, err := td.in.WriteTo(buf)
			if !errors.Is(err, td.writeErr) {
				t.Errorf("WriteTo(%+v) received err = '%v', expected '%v'", td.in, err, td.writeErr)
			} else if td.writeErr == nil {
				ha := header{}
				if _, err := ha.ReadFrom(buf); !errors.Is(err, td.readErr) {
					t.Errorf("ReadFrom received err = %v, expected %v", err, nil)
				}
				if ha != td.in {
					t.Errorf("header before does not match header after\nbefore: %+v\nafter: %+v", td.in, ha)
				}
			}


		})
	}
}


func TestHeaderChecksum(t *testing.T) {
	hb := header{
		Magic: defaultHeaderMagic,
		MajorVersion: 1,
		MinorVersion: 0,
		PackID: 4000,
		State: stateCompleted,
		HeaderSize: headerLen,
	}
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if _, err := hb.WriteTo(buf); err != nil {
		t.Fatalf("unexpected error for WriteTo: %v", err)
	}

	// modifying checksum so it does not match
	buf.Bytes()[ho8Checksum+1] = byte(0)

	ha := header{}
	if _, err := ha.ReadFrom(buf); !errors.Is(err, ErrHeaderChecksum) {
		t.Fatalf("ReadFrom received err '%v', want '%v'", err, ErrHeaderChecksum)
	}
}
