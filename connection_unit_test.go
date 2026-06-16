// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"testing"
	"time"
)

func TestConnectionBlockedDispatchDropsNotificationOnFullListener(t *testing.T) {
	t.Parallel()

	listener := make(chan Blocking, 1)
	listener <- Blocking{} // pre-fill to capacity

	c := &Connection{}
	c.blocks = append(c.blocks, listener)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.dispatch0(&methodFrame{Method: &connectionBlocked{Reason: "memory"}})
	}()

	select {
	case <-done:
		if len(listener) != 1 {
			t.Fatalf("expected listener to retain 1 item (notification dropped), got %d", len(listener))
		}
	case <-time.After(6 * time.Second):
		t.Fatal("connectionBlocked dispatch blocked on full listener for more than 6 seconds")
	}
}

func TestConnectionUnblockedDispatchDropsNotificationOnFullListener(t *testing.T) {
	t.Parallel()

	listener := make(chan Blocking, 1)
	listener <- Blocking{} // pre-fill to capacity

	c := &Connection{}
	c.blocks = append(c.blocks, listener)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.dispatch0(&methodFrame{Method: &connectionUnblocked{}})
	}()

	select {
	case <-done:
		if len(listener) != 1 {
			t.Fatalf("expected listener to retain 1 item (notification dropped), got %d", len(listener))
		}
	case <-time.After(6 * time.Second):
		t.Fatal("connectionUnblocked dispatch blocked on full listener for more than 6 seconds")
	}
}
