package local

import (
	"bytes"
	"testing"
)

func TestHeaderBytes(t *testing.T) {
	hb := header{
		Magic:   [4]byte{'P', 'A', 'C', 'K'},
		MajorVersion: 3,
		MinorVersion: 89,
		PackID:  89,
		State: stateCompacting,
		HeaderSize: 128,
	}

	buf := bytes.NewBuffer(make([]byte, 0, 128))
	hb.WriteTo(buf)

	ha := header{}
	ha.ReadFrom(buf)

	if hb != ha {
		t.Fatalf("header before does not match header after\nbefore: %+v\nafter: %+v", hb, ha)
	}
}
