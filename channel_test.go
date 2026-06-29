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

func TestRecvContentBodyPreallocationIsCappedToFrameSize(t *testing.T) {
	testCases := []struct {
		name       string
		headerSize uint64
		frameSize  int
		wantMaxCap int
	}{
		{
			// A server advertising a 1 MiB body into a connection negotiated
			// at 128 KiB must not pre-allocate more than FrameSize bytes.
			name:       "header size larger than FrameSize is capped",
			headerSize: 1 << 20, // 1 MiB declared body
			frameSize:  131072,  // 128 KiB negotiated FrameSize
			wantMaxCap: 131072,
		},
		{
			name:       "header size equal to FrameSize is not inflated",
			headerSize: 131072,
			frameSize:  131072,
			wantMaxCap: 131072,
		},
		{
			// When the declared body fits within FrameSize, use the exact
			// declared size as the pre-allocation hint.
			name:       "header size smaller than FrameSize uses header size",
			headerSize: 1024,
			frameSize:  131072,
			wantMaxCap: 1024,
		},
		{
			// FrameSize == 0 means "unlimited" per Config docs; fall back to
			// the declared header size.
			name:       "FrameSize zero uses header size",
			headerSize: 1024,
			frameSize:  0,
			wantMaxCap: 1024,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &Channel{
				connection: &Connection{Config: Config{FrameSize: tc.frameSize}},
				header:     &headerFrame{Size: tc.headerSize},
			}

			// Deliver a small body chunk that does not complete the message so
			// dispatch is not triggered; we only want to inspect the cap.
			ch.recvContent(&bodyFrame{Body: []byte("hello")})

			if cap(ch.body) > tc.wantMaxCap {
				t.Fatalf("body buffer cap = %d, want <= %d", cap(ch.body), tc.wantMaxCap)
			}
		})
	}
}

func TestQosNegativeReturnsError(t *testing.T) {
	ch := &Channel{}
	if err := ch.validateQos(-1, 0); err == nil {
		t.Fatal("expected error for negative prefetchCount")
	}
	if err := ch.validateQos(0, -1); err == nil {
		t.Fatal("expected error for negative prefetchSize")
	}
	if err := ch.validateQos(-1, -1); err == nil {
		t.Fatal("expected error for negative prefetchCount and prefetchSize")
	}
	if err := ch.validateQos(0, 0); err != nil {
		t.Fatalf("unexpected error for zero values: %v", err)
	}
	if err := ch.validateQos(10, 100); err != nil {
		t.Fatalf("unexpected error for positive values: %v", err)
	}
}

