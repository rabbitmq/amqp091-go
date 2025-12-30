package amqp091

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fieldTypeTest struct {
	fieldType byte
	value     interface{}
	encoded   string
}

// This function constructs a header frame given a payload string, using some sane defaults.
// A bit of reimplementing the code under test here, but it's not worth manually computing payload sizes for every
// test case; we primarily care about testing the encodings themselves.
func makeHeader(key string, encodedField string) string {
	payload := string([]byte{byte(len(key))}) + key + encodedField
	size := 18 + len(payload) // 18 is a magic number derived from the structure of our header frame
	tableLen := len(payload)
	sizeBytes := make([]byte, 4)
	tableLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(size))
	binary.BigEndian.PutUint32(tableLenBytes, uint32(tableLen))
	p := "\x02" + // frame type: header
		"\x00\x01" + // channel: 1
		string(sizeBytes) +
		"\x00\x3C" + // ClassId: 60 (basic)
		"\x00\x00" + // Weight: 0
		"\x00\x00\x00\x00\x00\x00\x00\x04" + // Body size: 4 bytes (irrelevant)
		"\x20\x00" + // Property flags: 8192 (headers present)
		string(tableLenBytes) +
		payload +
		"\xCE"
	return p
}

// Happy case tests
func TestReadWriteAllFieldTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("excessive allocation")
	}

	// Each of these test cases use hex to express their encoded values for explicitness. The sole exception is the
	// first character which is the field type itself, and cases where the field type is a string, in which case the
	// string is directly embedded in the encoded form for readability.
	fieldTests := []fieldTypeTest{
		// short strings, max 255 bytes
		{'S', "bar", "S\x00\x00\x00\x03bar"},
		{'S', "iamalongerstringbutstillunderthe255bytelimit", "S\x00\x00\x00\x2Ciamalongerstringbutstillunderthe255bytelimit"},
		// int32
		{'I', int32(-2147483648), "I\x80\x00\x00\x00"},
		{'I', int32(42), "I\x00\x00\x00\x2A"},
		{'I', int32(2147483647), "I\x7F\xFF\xFF\xFF"},
		// timestamp
		{'T', time.Unix(1234567890, 0), "T\x00\x00\x00\x00I\x96\x02\xd2"},
		// int8
		{'b', int8(-5), "b\xfb"},
		{'b', int8(5), "b\x05"},
		// byte
		{'B', byte(0x7f), "B\x7f"},
		// int16
		{'s', int16(-1234), "s\xfb\x2e"},
		{'s', int16(1234), "s\x04\xd2"},
		// int64
		{'l', int64(123456789012345), "l\x00\x00\x70\x48\x86\x0D\xDF\x79"},
		{'l', int64(-123456789012345), "l\xff\xff\x8f\xb7\x79\xf2\x20\x87"},
		// float32
		{'f', float32(3.14), "f\x40\x48\xf5\xc3"},
		// float64
		{'d', float64(2.71828), "d\x40\x05\xbf\x09\x95\xaa\xf7\x90"},
		// decimal
		{'D', Decimal{Scale: 2, Value: 314}, "D\x02\x00\x00\x01\x3A"},
		// array
		{'A', []interface{}{"foo", int32(1)}, "A\x00\x00\x00\x0d\x53\x00\x00\x00\x03\x66\x6F\x6F\x49\x00\x00\x00\x01"},
		// table
		{'F', Table{"baz": "qux"}, "F\x00\x00\x00\x0c\x03bazS\x00\x00\x00\x03qux"},
		// byte array
		{'x', []byte{0x01, 0x02, 0x03}, "x\x00\x00\x00\x03\x01\x02\x03"},
		// boolean
		{'t', true, "t\x01"},
		{'t', false, "t\x00"},
		// void
		{'V', nil, "V"},
		// uint16
		{'u', uint16(12345), "u\x30\x39"},
		{'u', uint16(65535), "u\xff\xff"},
		{'u', uint16(0), "u\x00\x00"},
		// uint32
		{'i', uint32(12345), "i\x00\x00\x30\x39"},
		{'i', uint32(4294967295), "i\xff\xff\xff\xff"},
		{'i', uint32(0), "i\x00\x00\x00\x00"},
	}

	// Run through each test case and exercise both read and write pathways, ensuring the string value and the encoded
	// value always match.
	for idx, ft := range fieldTests {
		input := makeHeader("foo", ft.encoded)
		t.Run(string(ft.fieldType), func(t *testing.T) {
			// Test reading
			r := reader{strings.NewReader(input)}
			frame, err := r.ReadFrame()
			if err != nil {
				t.Errorf("scenario %d: unexpected error for field type %q: %v", idx, ft.fieldType, err)
				return
			}
			hf, ok := frame.(*headerFrame)
			if !ok {
				t.Errorf("scenario %d: frame is not a headerFrame: %#v", idx, frame)
				return
			}
			got := hf.Properties.Headers["foo"]
			want := ft.value
			// Special handling for comparing equality of []byte and time.Time
			switch v := want.(type) {
			case []byte:
				if g, ok := got.([]byte); !ok || !bytes.Equal(g, v) {
					t.Errorf("scenario %d: unexpected []byte value: got %#v, want %#v", idx, got, v)
				}
			case time.Time:
				if g, ok := got.(time.Time); !ok || !g.Equal(v) {
					t.Errorf("scenario %d: unexpected time.Time value: got %#v, want %#v", idx, got, v)
				}
			default:
				if !reflect.DeepEqual(got, want) {
					t.Errorf("scenario %d: unexpected value: got %#v, want %#v", idx, got, want)
				}
			}

			// Test writing
			var buf bytes.Buffer
			hf = &headerFrame{
				ChannelId: 1,
				ClassId:   60,
				weight:    0,
				Size:      4,
				Properties: properties{
					Headers: Table{
						"foo": ft.value,
					},
				},
			}
			err = hf.write(&buf)
			if err != nil {
				t.Errorf("scenario %d: unexpected error for field type %q: %v", idx, ft.fieldType, err)
				return
			}
			writeGot := buf.Bytes()
			writeWant := []byte(input)
			if !bytes.Equal(writeGot, writeWant) {
				t.Errorf("scenario %d: unexpected encoding for field type %q: got: %x want: %x", idx, ft.fieldType, writeGot, writeWant)
			}
		})
	}
}

// We should throw if we encounter something on the wire that does not match our field type expectations.
// Note: No need to test bad writes as Go will enforce that you can't do uint16(65536) for example, it's too big to fit.
func TestReadWriteAllFieldTypesBadParse(t *testing.T) {
	if testing.Short() {
		t.Skip("excessive allocation")
	}

	fieldTests := []fieldTypeTest{
		// uint16
		{'u', uint16(12345), "u\x30\x39\x40"}, // Too many bytes
		{'u', uint16(12345), "u\x30"},         // Too few bytes
		// uint32
		{'i', uint32(12345), "i\x00\x00\x30\x39\x40"}, // Too many bytes
		{'i', uint32(12345), "i\x00\x00\x30"},         // Too few bytes
	}

	for idx, ft := range fieldTests {
		input := makeHeader("foo", ft.encoded)
		t.Run(string(ft.fieldType), func(t *testing.T) {
			// Test reading
			r := reader{strings.NewReader(input)}
			_, err := r.ReadFrame()
			if err == nil {
				t.Errorf("scenario %d: missing expected error for field type %q: %v", idx, ft.fieldType, err)
				return
			}
		})
	}
}
