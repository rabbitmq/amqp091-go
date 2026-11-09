// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func TestWriteShortstrTruncationReturnsError(t *testing.T) {
	longStr := strings.Repeat("a", 256) // 256 bytes > 255 max
	var buf bytes.Buffer
	err := writeShortstr(&buf, longStr)
	if err == nil {
		t.Fatal("expected error for shortstr > 255 bytes, got nil")
	}
}

func TestWriteShortstrExact255Succeeds(t *testing.T) {
	s := strings.Repeat("a", 255)
	var buf bytes.Buffer
	if err := writeShortstr(&buf, s); err != nil {
		t.Fatalf("unexpected error for 255-byte shortstr: %v", err)
	}
	if buf.Len() != 256 { // 1-byte length prefix + 255 bytes
		t.Fatalf("wrong output length: %d", buf.Len())
	}
}

// TestHeaderFrameReturnID checks injection without application headers.
func TestHeaderFrameReturnID(t *testing.T) {
	f := &headerFrame{ChannelId: 1, ClassId: 60, Properties: properties{returnID: 42}}
	var encoded bytes.Buffer

	if err := f.write(&encoded); err != nil {
		t.Fatal(err)
	}

	decoder := reader{r: &encoded}
	decoded, err := decoder.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}

	want := Table{returnIDHeader: binary.BigEndian.AppendUint64(nil, 42)}
	if got := decoded.(*headerFrame).Properties.Headers; !reflect.DeepEqual(got, want) {
		t.Fatalf("headers = %#v, want %#v", got, want)
	}
}

// TestWriteTableWithReturnID checks the wire encoding contains exactly one ID.
func TestWriteTableWithReturnID(t *testing.T) {
	for _, tc := range []struct {
		name     string
		headers  Table
		returnID uint64
		wantID   []byte
	}{
		{"injected", nil, 42, []byte{0, 0, 0, 0, 0, 0, 0, 42}},
		{"replaced", Table{returnIDHeader: "old"}, 42, []byte{0, 0, 0, 0, 0, 0, 0, 42}},
		{"max", nil, ^uint64(0), []byte{255, 255, 255, 255, 255, 255, 255, 255}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var encoded bytes.Buffer
			if err := writeTableWithReturnID(&encoded, tc.headers, tc.returnID); err != nil {
				t.Fatal(err)
			}

			want := []byte{0, 0, 0, 34, 20}
			want = append(want, returnIDHeader...)
			want = append(want, 'x', 0, 0, 0, 8)
			want = append(want, tc.wantID...)

			if !bytes.Equal(encoded.Bytes(), want) {
				t.Fatalf("encoded table = %v, want %v", encoded.Bytes(), want)
			}
		})
	}
}

func TestTableReturnIDFiltering(t *testing.T) {
	newTable := func() Table {
		return Table{
			returnIDHeader: "original",
			"ordinary":     "value",
			"nested":       Table{returnIDHeader: "nested value"},
			"array":        []any{Table{returnIDHeader: "array value"}},
		}
	}

	for _, tc := range []struct {
		name     string
		header   bool
		returnID uint64
	}{
		{name: "generic table"},
		{name: "publishing headers without generated ID", header: true},
		{name: "publishing headers with generated ID", header: true, returnID: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := newTable()
			want := newTable()
			var encoded bytes.Buffer
			var got Table

			if tc.header {
				delete(want, returnIDHeader)
				if tc.returnID != 0 {
					want[returnIDHeader] = binary.BigEndian.AppendUint64(nil, tc.returnID)
				}

				frame := &headerFrame{ChannelId: 1, ClassId: 60, Properties: properties{Headers: input, returnID: tc.returnID}}
				if err := frame.write(&encoded); err != nil {
					t.Fatal(err)
				}

				decoder := reader{r: &encoded}
				decoded, err := decoder.ReadFrame()
				if err != nil {
					t.Fatal(err)
				}
				got = decoded.(*headerFrame).Properties.Headers
			} else {
				if err := writeTable(&encoded, input); err != nil {
					t.Fatal(err)
				}

				var err error
				got, err = readTable(&encoded)
				if err != nil {
					t.Fatal(err)
				}
			}

			if !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded table = %#v, want %#v", got, want)
			}
			if !reflect.DeepEqual(input, newTable()) {
				t.Fatalf("encoding modified the input table: %#v", input)
			}
		})
	}
}
