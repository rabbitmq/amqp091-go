// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"strings"
	"testing"
)

func TestReadHeaderFrame(t *testing.T) {
	testData := []string{
		"\x02\x00\x01\x00\x00\x00\x12\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x54\x00\x00\x00\x00\x00\x00\xce",
	}

	for idx, testStr := range testData {
		r := reader{strings.NewReader(testStr)}
		_, err := r.ReadFrame()
		if err != nil {
			t.Errorf("%d. failed to read header frame: err = %v", idx, err)
		}
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
