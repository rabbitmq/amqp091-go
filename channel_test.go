// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"testing"
	"time"
)

func TestChannelFlowDispatchDropsNotificationOnFullListener(t *testing.T) {
	t.Parallel()

	listener := make(chan bool, 1)
	listener <- false // pre-fill to capacity

	ch := &Channel{consumers: makeConsumers()}
	ch.flows = append(ch.flows, listener)
	ch.closed.Store(true) // prevent send() from accessing nil connection

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.dispatch(&channelFlow{Active: true})
	}()

	select {
	case <-done:
		if len(listener) != 1 {
			t.Fatalf("expected listener to retain 1 item (notification dropped), got %d", len(listener))
		}
	case <-time.After(6 * time.Second):
		t.Fatal("channelFlow dispatch blocked on full listener for more than 6 seconds")
	}
}

func TestBasicCancelDispatchDropsNotificationOnFullListener(t *testing.T) {
	t.Parallel()

	listener := make(chan string, 1)
	listener <- "existing-tag" // pre-fill to capacity

	ch := &Channel{consumers: makeConsumers()}
	ch.cancels = append(ch.cancels, listener)
	ch.closed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.dispatch(&basicCancel{ConsumerTag: "new-tag"})
	}()

	select {
	case <-done:
		if len(listener) != 1 {
			t.Fatalf("expected listener to retain 1 item (notification dropped), got %d", len(listener))
		}
	case <-time.After(6 * time.Second):
		t.Fatal("basicCancel dispatch blocked on full listener for more than 6 seconds")
	}
}

func TestBasicReturnDispatchDropsNotificationOnFullListener(t *testing.T) {
	t.Parallel()

	listener := make(chan Return, 1)
	listener <- Return{} // pre-fill to capacity

	ch := &Channel{consumers: makeConsumers()}
	ch.returns = append(ch.returns, listener)
	ch.closed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.dispatch(&basicReturn{})
	}()

	select {
	case <-done:
		if len(listener) != 1 {
			t.Fatalf("expected listener to retain 1 item (notification dropped), got %d", len(listener))
		}
	case <-time.After(6 * time.Second):
		t.Fatal("basicReturn dispatch blocked on full listener for more than 6 seconds")
	}
}
