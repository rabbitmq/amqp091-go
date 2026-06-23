// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"strings"
	"testing"
	"time"
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

func TestCancelByQueue(t *testing.T) {
	subs := makeConsumers()
	defer subs.close()

	ch1 := make(chan Delivery, 1)
	ch2 := make(chan Delivery, 1)
	ch3 := make(chan Delivery, 1)

	subs.add("ctag1", ch1, consumerConfig{Queue: "queue1"})
	subs.add("ctag2", ch2, consumerConfig{Queue: "queue2"})
	subs.add("ctag3", ch3, consumerConfig{Queue: "queue1"})

	// Cancel all consumers on "queue1"
	subs.cancelByQueue("queue1")

	subs.Lock()
	_, found1 := subs.configs["ctag1"]
	_, found2 := subs.configs["ctag2"]
	_, found3 := subs.configs["ctag3"]
	subs.Unlock()

	if found1 {
		t.Error("Expected ctag1 to be removed from configs")
	}
	if !found2 {
		t.Error("Expected ctag2 to remain in configs")
	}
	if found3 {
		t.Error("Expected ctag3 to be removed from configs")
	}

	// Verify that the channels for canceled consumers were closed
	// (reading from closed channels returns immediately with ok=false)
	// We use a small timeout to allow concurrent goroutines to execute the deferred closes.
	select {
	case _, ok := <-ch1:
		if ok {
			t.Error("Expected ch1 to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected ch1 to be closed, but it blocked")
	}

	select {
	case _, ok := <-ch3:
		if ok {
			t.Error("Expected ch3 to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected ch3 to be closed, but it blocked")
	}

	// Verify ch2 remains open and we can write/read from it
	select {
	case _, ok := <-ch2:
		t.Errorf("Expected ch2 to remain open and block, but read succeeded (ok=%t)", ok)
	default:
		// Passed, ch2 is still open and blocked as expected
	}
}
