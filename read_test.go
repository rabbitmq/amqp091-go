// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
	r := reader{r: strings.NewReader(frame)}
	_, err := r.ReadFrame()
	if err != nil {
		t.Fatalf("expected no error reading header frame, got: %v", err)
	}
}

// TestReadFrameRejectsOversizedFrame verifies that a frame whose declared
// size exceeds the negotiated frame_max is rejected before its payload is
// allocated or read, preventing a malicious server from forcing large
// allocations by lying about a frame's size in the frame header.
func TestReadFrameRejectsOversizedFrame(t *testing.T) {
	const negotiatedMax = 4096 // total frame size, including header and frame-end byte

	header := make([]byte, 7)
	header[0] = frameBody
	binary.BigEndian.PutUint16(header[1:3], 1)
	binary.BigEndian.PutUint32(header[3:7], negotiatedMax) // payload alone already exceeds the limit

	var maxFrameSize atomic.Uint32
	maxFrameSize.Store(negotiatedMax)

	r := reader{r: bytes.NewReader(header), maxFrameSize: &maxFrameSize}
	frame, err := r.ReadFrame()
	if err != ErrFrameTooLarge {
		t.Fatalf("expected ErrFrameTooLarge, got frame=%#v err=%v", frame, err)
	}
}

// TestReadFrameAllowsUnlimitedWhenNegotiatedUnbounded verifies that a nil or
// zero-valued maxFrameSize (frame_max negotiated as unlimited, or not yet
// negotiated) does not reject frames, preserving prior behavior.
func TestReadFrameAllowsUnlimitedWhenNegotiatedUnbounded(t *testing.T) {
	header := make([]byte, 7)
	header[0] = frameBody
	binary.BigEndian.PutUint16(header[1:3], 1)
	binary.BigEndian.PutUint32(header[3:7], 3)

	buf := append(header, []byte("abc")...)
	buf = append(buf, frameEnd)

	r := reader{r: bytes.NewReader(buf)}
	if _, err := r.ReadFrame(); err != nil {
		t.Fatalf("expected no error with nil maxFrameSize, got: %v", err)
	}
}

// TestReadFrameAllowsAnySizeWhenMaxFrameSizeIsZero verifies that a non-nil
// maxFrameSize storing 0 (frame_max explicitly negotiated as unlimited, per
// negotiateFrameSize when both client and server request 0) does not reject
// frames, regardless of declared size.
func TestReadFrameAllowsAnySizeWhenMaxFrameSizeIsZero(t *testing.T) {
	const declaredSize = 1 << 20 // far larger than any realistic negotiated frame_max

	header := make([]byte, 7)
	header[0] = frameBody
	binary.BigEndian.PutUint16(header[1:3], 1)
	binary.BigEndian.PutUint32(header[3:7], declaredSize)

	payload := make([]byte, declaredSize)
	buf := append(header, payload...)
	buf = append(buf, frameEnd)

	var maxFrameSize atomic.Uint32 // zero value: unlimited

	r := reader{r: bytes.NewReader(buf), maxFrameSize: &maxFrameSize}
	if _, err := r.ReadFrame(); err != nil {
		t.Fatalf("expected no error with maxFrameSize == 0, got: %v", err)
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
		r := reader{r: strings.NewReader(testStr)}
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

// TestReadLongstrLargeDeclaredLengthReturnsErrorWithoutActualData verifies a
// long-str whose declared length exceeds the available data fails instead of
// forcing a large up-front allocation.
func TestReadLongstrLargeDeclaredLengthReturnsErrorWithoutActualData(t *testing.T) {
	// 256MiB claimed, far more than the data provided below.
	const declaredLength = 1 << 28

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, uint32(declaredLength)); err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}
	buf.WriteString("not nearly enough data")

	_, err := readLongstr(&buf)
	if err == nil {
		t.Fatal("expected error when declared length exceeds available data, got nil")
	}
}

// TestReadFieldByteArrayLargeDeclaredLengthReturnsErrorWithoutActualData is
// the 'x' (byte-array field) analogue of the longstr case above.
func TestReadFieldByteArrayLargeDeclaredLengthReturnsErrorWithoutActualData(t *testing.T) {
	const declaredLength = 1 << 28

	var buf bytes.Buffer
	buf.WriteByte('x')
	if err := binary.Write(&buf, binary.BigEndian, int32(declaredLength)); err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}
	buf.WriteString("not nearly enough data")

	_, err := readField(&buf)
	if err == nil {
		t.Fatal("expected error when declared length exceeds available data, got nil")
	}
}

// TestReadArrayOversizeLengthReturnsError verifies readArray rejects a
// declared size above max int32, matching readLongstr's existing guard.
func TestReadArrayOversizeLengthReturnsError(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, uint32(0x80000000)); err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}

	_, err := readArray(&buf)
	if err == nil {
		t.Fatal("expected error for oversized array length, got nil")
	}
}

// TestReadFieldDeepNestingReturnsErrorInsteadOfCrashing verifies a table
// nested far deeper than maxFieldDepth returns an error rather than
// recursing until the goroutine stack overflows.
func TestReadFieldDeepNestingReturnsErrorInsteadOfCrashing(t *testing.T) {
	var nested any = Table{"v": int32(1)}
	for i := 0; i < maxFieldDepth*4; i++ {
		nested = Table{"nested": nested}
	}

	var buf bytes.Buffer
	if err := writeField(&buf, nested); err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}

	_, err := readField(&buf)
	if err == nil {
		t.Fatal("expected error for deeply nested table, got nil")
	}
}

// TestReadArrayRejectsExcessiveElementCount verifies readArray rejects an
// array containing more than maxContainerElements entries, guarding against
// the empty-value ('V') padding trick that would otherwise amplify a small
// number of wire bytes into a disproportionately large []any allocation.
func TestReadArrayRejectsExcessiveElementCount(t *testing.T) {
	elems := make([]any, maxContainerElements+1)

	var buf bytes.Buffer
	if err := writeField(&buf, elems); err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}

	if _, err := readField(&buf); err == nil {
		t.Fatal("expected error for array exceeding maxContainerElements, got nil")
	}
}

// TestReadArrayAcceptsElementCountAtCap verifies readArray still accepts an
// array with exactly maxContainerElements entries, i.e. the cap doesn't
// reject legitimate, if generous, arrays.
func TestReadArrayAcceptsElementCountAtCap(t *testing.T) {
	elems := make([]any, maxContainerElements)

	var buf bytes.Buffer
	if err := writeField(&buf, elems); err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}

	value, err := readField(&buf)
	if err != nil {
		t.Fatalf("expected no error at maxContainerElements, got: %v", err)
	}

	arr, ok := value.([]any)
	if !ok || len(arr) != maxContainerElements {
		t.Fatalf("expected array of length %d, got %#v", maxContainerElements, value)
	}
}

// TestReadTableRejectsExcessiveEntryCount is the Table analogue of
// TestReadArrayRejectsExcessiveElementCount.
func TestReadTableRejectsExcessiveEntryCount(t *testing.T) {
	table := make(Table, maxContainerElements+1)
	for i := 0; i < maxContainerElements+1; i++ {
		table[fmt.Sprintf("%d", i)] = nil
	}

	var buf bytes.Buffer
	if err := writeTable(&buf, table); err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}

	if _, err := readTable(&buf); err == nil {
		t.Fatal("expected error for table exceeding maxContainerElements, got nil")
	}
}

// TestReadTableAcceptsEntryCountAtCap verifies readTable still accepts a
// table with exactly maxContainerElements entries.
func TestReadTableAcceptsEntryCountAtCap(t *testing.T) {
	table := make(Table, maxContainerElements)
	for i := 0; i < maxContainerElements; i++ {
		table[fmt.Sprintf("%d", i)] = nil
	}

	var buf bytes.Buffer
	if err := writeTable(&buf, table); err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}

	output, err := readTable(&buf)
	if err != nil {
		t.Fatalf("expected no error at maxContainerElements, got: %v", err)
	}
	if len(output) != maxContainerElements {
		t.Fatalf("expected table of length %d, got %d", maxContainerElements, len(output))
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

func fuzzSeedTable() Table {
	return Table{
		"bool":      true,
		"byte":      byte(0xff),
		"int8":      int8(-1),
		"int16":     int16(-2),
		"int32":     int32(-3),
		"int64":     int64(-4),
		"uint16":    uint16(5),
		"uint32":    uint32(6),
		"float32":   float32(1.5),
		"float64":   float64(-2.5),
		"decimal":   Decimal{Scale: 2, Value: 12345},
		"string":    "string",
		"bytes":     []byte("bytes"),
		"timestamp": time.Unix(1700000000, 0),
		"table":     Table{"nested": "value"},
		"array":     []any{int32(1), "two", nil, []any{}},
		"void":      nil,
	}
}

func fuzzSeedFrames(t testing.TB) [][]byte {
	props := properties{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Headers:         fuzzSeedTable(),
		DeliveryMode:    Persistent,
		Priority:        9,
		CorrelationId:   "correlation",
		ReplyTo:         "reply",
		Expiration:      "60000",
		MessageId:       "message",
		Timestamp:       time.Unix(1700000000, 0),
		Type:            "type",
		UserId:          "guest",
		AppId:           "app",
	}

	frames := []frame{
		&methodFrame{ChannelId: 0, Method: &connectionStart{
			VersionMajor:     0,
			VersionMinor:     9,
			ServerProperties: fuzzSeedTable(),
			Mechanisms:       "PLAIN AMQPLAIN",
			Locales:          "en_US",
		}},
		&methodFrame{ChannelId: 1, Method: &basicPublish{Exchange: "exchange", RoutingKey: "key", Mandatory: true}},
		&methodFrame{ChannelId: 1, Method: &basicDeliver{ConsumerTag: "ctag", DeliveryTag: 42, Redelivered: true, Exchange: "exchange", RoutingKey: "key"}},
		&headerFrame{ChannelId: 1, ClassId: 60, Size: 5, Properties: props},
		&bodyFrame{ChannelId: 1, Body: []byte("hello")},
		&heartbeatFrame{},
	}

	seeds := make([][]byte, 0, len(frames))
	for _, f := range frames {
		var buf bytes.Buffer
		if err := f.write(&buf); err != nil {
			t.Fatalf("failed to build seed frame %#v: %v", f, err)
		}
		seeds = append(seeds, buf.Bytes())
	}
	return seeds
}

func fuzzSeedFields(t testing.TB) [][]byte {
	var nested any = Table{}
	for i := 0; i <= maxFieldDepth; i++ {
		nested = Table{"n": nested}
	}

	var seeds [][]byte
	for _, v := range []any{fuzzSeedTable(), []any{fuzzSeedTable(), fuzzSeedTable()}, nested} {
		var buf bytes.Buffer
		if err := writeField(&buf, v); err != nil {
			t.Fatalf("failed to build seed field: %v", err)
		}
		seeds = append(seeds, buf.Bytes())
	}

	for _, tag := range []byte("tBbsIluifdDSATFxV?") {
		seeds = append(seeds, []byte{tag})
	}

	return append(seeds,
		[]byte{},
		[]byte{'x', 0xff, 0xff, 0xff, 0xff},
		[]byte{'S', 0x80, 0x00, 0x00, 0x00},
		[]byte{'A', 0x80, 0x00, 0x00, 0x00},
		[]byte{'F', 0x7f, 0xff, 0xff, 0xff},
	)
}

// containsNaN reports whether v holds a NaN float anywhere, since
// reflect.DeepEqual never considers NaN equal to itself.
func containsNaN(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return math.IsNaN(v.Float())
	case reflect.Pointer, reflect.Interface:
		return !v.IsNil() && containsNaN(v.Elem())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if containsNaN(v.Field(i)) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if containsNaN(v.Index(i)) {
				return true
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			if containsNaN(iter.Value()) {
				return true
			}
		}
	}
	return false
}

func assertRoundTrip(t *testing.T, want, got any) {
	t.Helper()
	if !containsNaN(reflect.ValueOf(want)) && !reflect.DeepEqual(want, got) {
		t.Fatalf("round-trip mismatch:\nwant %#v\ngot  %#v", want, got)
	}
}

func checkFieldLimits(t *testing.T, v any, level int) {
	t.Helper()
	switch v := v.(type) {
	case Table:
		level++
		if len(v) > maxContainerElements {
			t.Fatalf("table has %d entries, limit is %d", len(v), maxContainerElements)
		}
		for _, e := range v {
			checkFieldLimits(t, e, level)
		}
	case []any:
		level++
		if len(v) > maxContainerElements {
			t.Fatalf("array has %d elements, limit is %d", len(v), maxContainerElements)
		}
		for _, e := range v {
			checkFieldLimits(t, e, level)
		}
	}
	if level > maxFieldDepth+1 {
		t.Fatalf("nesting level %d exceeds limit %d", level, maxFieldDepth+1)
	}
}

func FuzzReadFrame(f *testing.F) {
	for _, seed := range fuzzSeedFrames(f) {
		f.Add(seed, uint32(0))
		f.Add(seed, uint32(frameMinSize))
	}
	f.Add([]byte("\x02\x00\x01\x00\x00\x00\x12\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x54\x00\x00\x00\x00\x00\x00\xce"), uint32(0))
	f.Add([]byte("\b000000"), uint32(0))
	f.Add([]byte("\x02\x16\x10�[��\t\xbdui�"+"\x10\x01\x00\xff\xbf\xef\xbfｻn\x99\x00\x10r"), uint32(0))
	f.Add([]byte("\x0300\x00\x00\x00\x040000"), uint32(0))

	f.Fuzz(func(t *testing.T, data []byte, maxFrameSize uint32) {
		// ReadFrame relies on any nonzero limit being at least frameMinSize.
		if maxFrameSize != 0 && maxFrameSize < frameMinSize {
			maxFrameSize = frameMinSize
		}
		var max atomic.Uint32
		max.Store(maxFrameSize)

		r := reader{r: bytes.NewReader(data), maxFrameSize: &max}
		f1, err := r.ReadFrame()
		if err != nil {
			if f1 != nil {
				t.Fatalf("frame is not nil on error %v: %#v", err, f1)
			}
			return
		}

		if size := binary.BigEndian.Uint32(data[3:7]); maxFrameSize > 0 && size > maxFrameSize-frameHeaderSize {
			t.Fatalf("accepted frame of size %d with limit %d", size, maxFrameSize)
		}
		if channel := binary.BigEndian.Uint16(data[1:3]); f1.channel() != channel {
			t.Fatalf("frame channel %d, header channel %d", f1.channel(), channel)
		}

		rewrite := func(in frame) frame {
			t.Helper()
			var buf bytes.Buffer
			if err := in.write(&buf); err != nil {
				t.Fatalf("writing decoded frame %#v: %v", in, err)
			}
			out, err := (&reader{r: &buf}).ReadFrame()
			if err != nil {
				t.Fatalf("re-reading written frame %#v: %v", in, err)
			}
			if buf.Len() != 0 {
				t.Fatalf("%d trailing bytes after re-reading frame %#v", buf.Len(), in)
			}
			return out
		}

		// Writing normalizes some fields (e.g. empty properties), so compare
		// the second and third decodes.
		f2 := rewrite(f1)
		f3 := rewrite(f2)
		assertRoundTrip(t, f2, f3)
	})
}

func FuzzReadBytes(f *testing.F) {
	f.Add([]byte{}, int64(0))
	f.Add([]byte("abc"), int64(-1))
	f.Add([]byte("abc"), int64(2))
	f.Add([]byte("abc"), int64(4))
	f.Add(make([]byte, readAllocChunk+1), int64(readAllocChunk+1))
	f.Add([]byte("abc"), int64(math.MaxInt64))

	f.Fuzz(func(t *testing.T, data []byte, n int64) {
		got, err := readBytes(bytes.NewReader(data), n)
		switch {
		case n < 0:
			if !errors.Is(err, ErrSyntax) {
				t.Fatalf("expected ErrSyntax for n=%d, got %v", n, err)
			}
		case int64(len(data)) >= n:
			if err != nil {
				t.Fatalf("unexpected error for n=%d with %d bytes: %v", n, len(data), err)
			}
			if !bytes.Equal(got, data[:n]) {
				t.Fatalf("expected %q, got %q", data[:n], got)
			}
		default:
			if err == nil {
				t.Fatalf("expected error for n=%d with only %d bytes", n, len(data))
			}
		}
	})
}

func FuzzReadShortstr(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte("\x05hello"))
	f.Add([]byte("\x05hell"))
	f.Add(append([]byte{0xff}, bytes.Repeat([]byte{'a'}, 255)...))

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := readShortstr(bytes.NewReader(data))
		if len(data) == 0 || len(data) < 1+int(data[0]) {
			if err == nil {
				t.Fatalf("expected error for truncated shortstr %q", data)
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		encoded := data[:1+int(data[0])]
		if got != string(encoded[1:]) {
			t.Fatalf("expected %q, got %q", encoded[1:], got)
		}

		var buf bytes.Buffer
		if err := writeShortstr(&buf, got); err != nil {
			t.Fatalf("writeShortstr: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), encoded) {
			t.Fatalf("expected encoding %q, got %q", encoded, buf.Bytes())
		}
	})
}

func FuzzReadLongstr(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte("\x00\x00\x00\x05hello"))
	f.Add([]byte("\x00\x00\x00\x05hell"))
	f.Add([]byte{0x80, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := readLongstr(bytes.NewReader(data))
		if len(data) < 4 {
			if err == nil {
				t.Fatalf("expected error for truncated length %q", data)
			}
			return
		}

		length := binary.BigEndian.Uint32(data[:4])
		switch {
		case length > math.MaxInt32:
			if !errors.Is(err, ErrSyntax) {
				t.Fatalf("expected ErrSyntax for length %d, got %v", length, err)
			}
			return
		case uint64(len(data)) < 4+uint64(length):
			if err == nil {
				t.Fatalf("expected error for length %d with only %d bytes", length, len(data))
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		encoded := data[:4+length]
		if got != string(encoded[4:]) {
			t.Fatalf("expected %q, got %q", encoded[4:], got)
		}

		var buf bytes.Buffer
		if err := writeLongstr(&buf, got); err != nil {
			t.Fatalf("writeLongstr: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), encoded) {
			t.Fatalf("expected encoding %q, got %q", encoded, buf.Bytes())
		}
	})
}

func FuzzReadDecimal(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{2, 0, 0, 0x30, 0x39})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{2, 0, 0, 0x30})

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := readDecimal(bytes.NewReader(data))
		if len(data) < 5 {
			if err == nil {
				t.Fatalf("expected error for %d bytes", len(data))
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := Decimal{Scale: data[0], Value: int32(binary.BigEndian.Uint32(data[1:5]))}
		if got != want {
			t.Fatalf("expected %#v, got %#v", want, got)
		}
	})
}

func FuzzReadTimestamp(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0x65, 0x53, 0xf1, 0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := readTimestamp(bytes.NewReader(data))
		if len(data) < 8 {
			if err == nil {
				t.Fatalf("expected error for %d bytes", len(data))
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if want := int64(binary.BigEndian.Uint64(data[:8])); got.Unix() != want {
			t.Fatalf("expected %d, got %d", want, got.Unix())
		}
	})
}

func FuzzReadField(f *testing.F) {
	for _, seed := range fuzzSeedFields(f) {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := readField(bytes.NewReader(data))
		if err != nil {
			return
		}
		checkFieldLimits(t, v, 0)

		var buf bytes.Buffer
		if err := writeField(&buf, v); err != nil {
			t.Fatalf("writeField(%#v): %v", v, err)
		}
		got, err := readField(&buf)
		if err != nil {
			t.Fatalf("re-reading %#v: %v", v, err)
		}
		assertRoundTrip(t, v, got)
	})
}

func FuzzReadTable(f *testing.F) {
	for _, seed := range fuzzSeedFields(f) {
		if len(seed) > 0 && seed[0] == 'F' {
			f.Add(seed[1:])
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := readTable(bytes.NewReader(data))
		if err != nil {
			return
		}
		checkFieldLimits(t, v, 0)

		var buf bytes.Buffer
		if err := writeTable(&buf, v); err != nil {
			t.Fatalf("writeTable(%#v): %v", v, err)
		}
		got, err := readTable(&buf)
		if err != nil {
			t.Fatalf("re-reading %#v: %v", v, err)
		}
		assertRoundTrip(t, v, got)
	})
}

func FuzzReadArray(f *testing.F) {
	for _, seed := range fuzzSeedFields(f) {
		if len(seed) > 0 && seed[0] == 'A' {
			f.Add(seed[1:])
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 5, 'S', 0, 0, 0, 9})

	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := readArray(bytes.NewReader(data))
		if err != nil {
			return
		}
		checkFieldLimits(t, v, 0)

		var buf bytes.Buffer
		if err := writeField(&buf, v); err != nil {
			t.Fatalf("writeField(%#v): %v", v, err)
		}
		if tag, _ := buf.ReadByte(); tag != 'A' {
			t.Fatalf("expected array tag 'A', got %q", tag)
		}
		got, err := readArray(&buf)
		if err != nil {
			t.Fatalf("re-reading %#v: %v", v, err)
		}
		assertRoundTrip(t, v, got)
	})
}
