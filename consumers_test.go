// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"strings"
	"testing"
)

func TestGeneratedUniqueConsumerTagDoesNotExceedMaxLength(t *testing.T) {
	assertCorrectLength := func(commandName string) {
		tag := commandNameBasedUniqueConsumerTag(commandName)
		if len(tag) > consumerTagLengthMax {
			t.Error("Generated unique consumer tag exceeds maximum length:", tag)
		}
	}

	assertCorrectLength("test")
	assertCorrectLength(strings.Repeat("z", 249))
	assertCorrectLength(strings.Repeat("z", 256))
	assertCorrectLength(strings.Repeat("z", 1024))
}
