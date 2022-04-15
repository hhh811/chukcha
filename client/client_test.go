package client

import (
	"chukcha/protocol"
	"reflect"
	"testing"
)

func TestSimpleMarshalUnmarshal(t *testing.T) {
	st := &state{
		Offsets: map[string]*ReadOffset{
			"h1": {
				CurChunk: protocol.Chunk{
					Name:     "h1-test000001",
					Complete: true,
					Size:     123456,
				},
				Off: 123,
			},
			"h2": {
				CurChunk: protocol.Chunk{
					Name:     "h2-test000002",
					Complete: true,
					Size:     100000,
				},
				Off: 100,
			},
		},
	}

	cl := NewSimple([]string{"http://localhost"})
	cl.st = st

	buf, err := cl.MarshalState()
	if err != nil {
		t.Errorf("MarshalState() = ..., %v; want no errors", err)
	}

	if l := len(buf); l <= 2 {
		t.Errorf("MarshalState90 = byte slice of length %d, ...; want length to be at list 3", l)
	}

	cl = NewSimple([]string{"http://localhost"})
	err = cl.RestoreSavedState(buf)

	if err != nil {
		t.Errorf("RestoreSavedState(...) = %v; want no errors", err)
	}

	if !reflect.DeepEqual(st, cl.st) {
		t.Errorf("RestoreSavedState() restored %+v; want %+v", cl.st, st)
	}
}
