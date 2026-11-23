// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bytes"
	"math"
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

func TestWriteFieldIntBeyondInt32KeepsValue(t *testing.T) {
	big := int64(math.MaxInt32) + 1
	if int64(int(big)) != big {
		t.Skip("int is 32 bits wide")
	}

	var buf bytes.Buffer
	if err := writeField(&buf, int(big)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := readField(&buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != big {
		t.Fatalf("expected %d, got %v", big, got)
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
