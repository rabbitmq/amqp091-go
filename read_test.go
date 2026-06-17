// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

// TestParseHeaderFrameConsumesPaddingBytes verifies that parseHeaderFrame
// reads exactly `size` bytes from the stream, even when the property flags
// indicate fewer bytes than the frame payload contains. Trailing bytes that
// are not consumed by property parsing must be drained so that the next
// ReadFrame call starts at the correct offset.
//
// The frame below is a real-world header frame (channel 1, body size 10,
// flags 0x5400 = ContentEncoding|DeliveryMode|CorrelationId) whose payload
// is 18 bytes but whose property fields only consume 15 bytes, leaving 3
// padding bytes before the frame-end octet.
func TestParseHeaderFrameConsumesPaddingBytes(t *testing.T) {
	frame := "\x02\x00\x01\x00\x00\x00\x12\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x54\x00\x00\x00\x00\x00\x00\xce"
	r := reader{strings.NewReader(frame)}
	_, err := r.ReadFrame()
	if err != nil {
		t.Fatalf("expected no error reading header frame, got: %v", err)
	}
}

func TestGoFuzzCrashers(t *testing.T) {
	if testing.Short() {
		t.Skip("excessive allocation")
	}

	testData := []string{
		"\b000000",
		"\x02\x16\x10�[��\t\xbdui�" + "\x10\x01\x00\xff\xbf\xef\xbfｻn\x99\x00\x10r",
		"\x0300\x00\x00\x00\x040000",
	}

	for idx, testStr := range testData {
		r := reader{strings.NewReader(testStr)}
		frame, err := r.ReadFrame()
		if err != nil && frame != nil {
			t.Errorf("%d. frame is not nil: %#v err = %v", idx, frame, err)
		}
	}
}

func TestReadFieldUnsignedTypes(t *testing.T) {
	testCases := []struct {
		name     string
		encoded  []byte
		expected any
	}{
		{name: "short-uint zero", encoded: []byte{'u', 0x00, 0x00}, expected: uint16(0)},
		{name: "short-uint max", encoded: []byte{'u', 0xff, 0xff}, expected: uint16(65535)},
		{name: "long-uint zero", encoded: []byte{'i', 0x00, 0x00, 0x00, 0x00}, expected: uint32(0)},
		{name: "long-uint max", encoded: []byte{'i', 0xff, 0xff, 0xff, 0xff}, expected: uint32(4294967295)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := readField(bytes.NewReader(tc.encoded))
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}

			if !reflect.DeepEqual(tc.expected, value) {
				t.Fatalf("expected %#v (%T), got %#v (%T)", tc.expected, tc.expected, value, value)
			}
		})
	}
}

func TestReadFieldByteArrayNegativeLength(t *testing.T) {
	testCases := []struct {
		name    string
		encoded []byte
	}{
		{
			name: "negative-one",
			// 'x' type tag + int32(-1) big-endian = 0xFFFFFFFF
			encoded: []byte{'x', 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name: "min-int32",
			// 'x' type tag + int32(math.MinInt32) big-endian = 0x80000000
			encoded: []byte{'x', 0x80, 0x00, 0x00, 0x00},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Catch any panic so a failure reports as a test error, not a crash.
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("readField panicked with negative byte-array length: %v", r)
				}
			}()

			_, err := readField(bytes.NewReader(tc.encoded))
			if err == nil {
				t.Fatal("expected error for negative byte-array length, got nil")
			}
		})
	}
}

func TestReadLongstrOversizeLengthReturnsError(t *testing.T) {
	testCases := []struct {
		name    string
		encoded []byte
	}{
		{
			// 0x80000000 = max_int32 + 1 — first value past the accepted range.
			name:    "max-int32-plus-one",
			encoded: []byte{0x80, 0x00, 0x00, 0x00},
		},
		{
			// 0xFFFFFFFF = max uint32 — largest possible declared length.
			name:    "max-uint32",
			encoded: []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readLongstr(bytes.NewReader(tc.encoded))
			if err == nil {
				t.Fatal("expected error for oversized longstr length, got nil")
			}
		})
	}
}

func TestReadTableOversizeOuterBlobReturnsError(t *testing.T) {
	// Wire layout: [uint32: 0x80000000] [bytes that represent the "table blob"].
	// The uint32 is the declared size of the table blob, which exceeds max_int32.
	// These bytes should never be reached as valid frame data after the fix.
	var buf bytes.Buffer
	buf.Write([]byte{0x80, 0x00, 0x00, 0x00}) // declared size = max_int32 + 1
	buf.WriteString("sentinel frame data")    // bytes that must not be misread

	_, err := readTable(&buf)
	if err == nil {
		t.Fatal("readTable with oversized outer blob must return error, not silent empty table")
	}
}

func TestWriteFieldUnsignedTypes(t *testing.T) {
	testCases := []struct {
		name     string
		value    any
		expected []byte
	}{
		{name: "short-uint zero", value: uint16(0), expected: []byte{'u', 0x00, 0x00}},
		{name: "short-uint max", value: uint16(65535), expected: []byte{'u', 0xff, 0xff}},
		{name: "long-uint zero", value: uint32(0), expected: []byte{'i', 0x00, 0x00, 0x00, 0x00}},
		{name: "long-uint max", value: uint32(4294967295), expected: []byte{'i', 0xff, 0xff, 0xff, 0xff}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeField(&buf, tc.value); err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}

			if !bytes.Equal(tc.expected, buf.Bytes()) {
				t.Fatalf("expected %v, got %v", tc.expected, buf.Bytes())
			}
		})
	}
}

func TestTableRoundTripUnsignedTypes(t *testing.T) {
	input := Table{
		"short": uint16(65535),
		"long":  uint32(4294967295),
	}

	var buf bytes.Buffer
	if err := writeTable(&buf, input); err != nil {
		t.Fatalf("writeTable failed: %v", err)
	}

	output, err := readTable(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("readTable failed: %v", err)
	}

	for key, expected := range input {
		got, ok := output[key]
		if !ok {
			t.Fatalf("missing key %q after round-trip", key)
		}
		if !reflect.DeepEqual(expected, got) {
			t.Fatalf("key %q mismatch: expected %#v (%T), got %#v (%T)", key, expected, expected, got, got)
		}
	}
}
