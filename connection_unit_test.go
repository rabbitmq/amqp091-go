// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"errors"
	"net"
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

func TestConnectionOpenCompleteZerosSASLCredentials(t *testing.T) {
	pa := &PlainAuth{Username: "user", Password: "secretpassword"}
	apa := &AMQPlainAuth{Username: "user", Password: "secretpassword"}

	c := &Connection{
		Config: Config{
			SASL: []Authentication{pa, apa},
		},
	}

	err := c.openComplete()
	if err != nil {
		t.Fatalf("unexpected error from openComplete: %v", err)
	}

	if pa.Password != "" {
		t.Errorf("expected PlainAuth password to be zeroed out, got %q", pa.Password)
	}

	if apa.Password != "" {
		t.Errorf("expected AMQPlainAuth password to be zeroed out, got %q", apa.Password)
	}
}

func TestConfigSetSASL(t *testing.T) {
	uri, err := ParseURI("amqp://guest:secret@localhost:5672/")
	if err != nil {
		t.Fatalf("failed to parse URI: %v", err)
	}

	config := Config{}
	err = config.setSASL(uri)
	if err != nil {
		t.Fatalf("unexpected error from setSASL: %v", err)
	}

	if len(config.SASL) != 1 {
		t.Fatalf("expected 1 SASL mechanism, got %d", len(config.SASL))
	}

	pa, ok := config.SASL[0].(*PlainAuth)
	if !ok {
		t.Fatalf("expected PlainAuth, got %T", config.SASL[0])
	}

	if pa.Username != "guest" || pa.Password != "secret" {
		t.Errorf("expected guest:secret, got %s:%s", pa.Username, pa.Password)
	}
}

func TestReconnectRestoresSASLCredentials(t *testing.T) {
	pa := &PlainAuth{Username: "user", Password: ""} // zeroed out
	c := &Connection{
		url:       "amqp://user:mysecretpassword@localhost:5672/",
		lifeCycle: newLifeCycle(),
		Config: Config{
			SASL: []Authentication{pa},
			Recovery: &Recovery{
				ReconnectionConfig: &ReconnectionConfig{
					MaxRetryCount:         1,
					RetryInterval:         1 * time.Millisecond,
					RecoverableErrorCodes: []int{320}, // some recoverable code
				},
			},
			Dial: func(network, addr string) (net.Conn, error) {
				return nil, errors.New("mock dial error")
			},
		},
	}
	c.closed.Store(true)

	// Trigger Reconnect. It will fail due to the mock dialer, but should restore SASL first
	_ = c.Reconnect()

	if len(c.Config.SASL) != 1 {
		t.Fatalf("expected 1 SASL mechanism after Reconnect, got %d", len(c.Config.SASL))
	}

	restoredPa, ok := c.Config.SASL[0].(*PlainAuth)
	if !ok {
		t.Fatalf("expected PlainAuth after Reconnect, got %T", c.Config.SASL[0])
	}

	if restoredPa.Username != "user" || restoredPa.Password != "mysecretpassword" {
		t.Errorf("expected restored credentials to be user:mysecretpassword, got %s:%s", restoredPa.Username, restoredPa.Password)
	}
}

func TestNegotiationEnforcesFrameMinSize(t *testing.T) {
	tests := []struct {
		name             string
		clientFrameSize  int
		serverFrameMax   int
		expectedFrameMin int
	}{
		{
			name:             "client unlimited, server below min",
			clientFrameSize:  0,
			serverFrameMax:   1,
			expectedFrameMin: frameMinSize,
		},
		{
			name:             "client below min, server unlimited",
			clientFrameSize:  2048,
			serverFrameMax:   0,
			expectedFrameMin: frameMinSize,
		},
		{
			name:             "client unlimited, server unlimited",
			clientFrameSize:  0,
			serverFrameMax:   0,
			expectedFrameMin: 0,
		},
		{
			name:             "client and server above min (client smaller)",
			clientFrameSize:  8192,
			serverFrameMax:   16384,
			expectedFrameMin: 8192,
		},
		{
			name:             "client and server above min (server smaller)",
			clientFrameSize:  16384,
			serverFrameMax:   8192,
			expectedFrameMin: 8192,
		},
		{
			name:             "client and server below min",
			clientFrameSize:  1,
			serverFrameMax:   1,
			expectedFrameMin: frameMinSize,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := negotiateFrameSize(tc.clientFrameSize, tc.serverFrameMax)
			if got != tc.expectedFrameMin {
				t.Errorf("negotiateFrameSize(%d, %d) = %d; want %d", tc.clientFrameSize, tc.serverFrameMax, got, tc.expectedFrameMin)
			}
		})
	}
}
