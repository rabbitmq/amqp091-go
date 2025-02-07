// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bytes"
	"strings"
	"testing"
)

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

func FuzzReadFrame(f *testing.F) {

	f.Add([]byte("\b000000"))
	f.Add([]byte("\x02\x16\x10�[��\t\xbdui�" + "\x10\x01\x00\xff\xbf\xef\xbfｻn\x99\x00\x10r"))
	f.Add([]byte("\x0300\x00\x00\x00\x040000"))

	f.Fuzz(func(t *testing.T, input_data []byte) {
		r := reader{bytes.NewReader(input_data)}
		_, _ = r.ReadFrame()
	})
}
