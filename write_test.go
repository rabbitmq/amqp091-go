// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bytes"
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
