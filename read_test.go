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
