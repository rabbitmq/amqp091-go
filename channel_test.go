// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"testing"
	"time"
)

// The following tests verify that synchronous sends to full notification
// channels do not block the reader goroutine.  Each test pre-fills the
// listener channel to capacity so that a bare `c <- value` would deadlock,
// then calls the dispatch handler in a goroutine and asserts it returns
// within a short timeout.

func TestChannelFlowDispatchDoesNotBlockOnFullListener(t *testing.T) {
	flowCh := make(chan bool, 1)
	flowCh <- true // pre-fill to capacity

	ch := &Channel{flows: []chan bool{flowCh}}
	ch.closed.Store(true) // ch.send() returns ErrClosed without accessing connection

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.dispatch(&channelFlow{Active: false})
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channelFlow dispatch blocked on full notification channel — reader goroutine would stall")
	}
}

func TestBasicCancelDispatchDoesNotBlockOnFullListener(t *testing.T) {
	cancelCh := make(chan string, 1)
	cancelCh <- "old-tag" // pre-fill to capacity

	ch := &Channel{
		cancels:   []chan string{cancelCh},
		consumers: makeConsumers(),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.dispatch(&basicCancel{ConsumerTag: "my-tag"})
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("basicCancel dispatch blocked on full notification channel — reader goroutine would stall")
	}
}

func TestBasicReturnDispatchDoesNotBlockOnFullListener(t *testing.T) {
	returnCh := make(chan Return, 1)
	returnCh <- Return{} // pre-fill to capacity

	ch := &Channel{returns: []chan Return{returnCh}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.dispatch(&basicReturn{})
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("basicReturn dispatch blocked on full notification channel — reader goroutine would stall")
	}
}
