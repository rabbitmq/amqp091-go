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

const (
	testQueue1             = "test-queue-1"
	testQueue2             = "test-queue-2"
	testQueue3             = "test-queue-3"
	testExchange1          = "test-exchange-1"
	testExchange2          = "test-exchange-2"
	testExchange3          = "test-exchange-3"
	testExchange4          = "test-exchange-4"
	testExchangeKindDirect = "direct"
	testExchangeKindTopic  = "topic"
	testKey1               = "test-key-1"
	testKey2               = "test-key-2"
	testKey3               = "test-key-3"
	testKeyEb1             = "test-key-eb-1"
	testKeyEb2             = "test-key-eb-2"
	testKeyEb3             = "test-key-eb-3"
	testKeyEb4             = "test-key-eb-4"
)

func compareBinding(b1, b2 BindingConfig) bool {
	return b1.Queue == b2.Queue && b1.Exchange == b2.Exchange && b1.Key == b2.Key
}

func compareExchangeBinding(eb1, eb2 ExchangeBindingConfig) bool {
	return eb1.Source == eb2.Source && eb1.Destination == eb2.Destination && eb1.Key == eb2.Key
}

func TestRecordAndRemoveQueue(t *testing.T) {
	ch := &Channel{}

	qc1 := QueueConfig{ActualName: testQueue1, Durable: true}
	qc2 := QueueConfig{ActualName: testQueue2, AutoDelete: true}

	ch.recordQueue(qc1)
	ch.recordQueue(qc2)

	config := ch.TopologyConfiguration()
	if len(config.Queues) != 2 {
		t.Fatalf("expected 2 queues, got %d", len(config.Queues))
	}
	if q, ok := config.Queues[testQueue1]; !ok || !q.Durable {
		t.Errorf("%s not recorded correctly", testQueue1)
	}
	if q, ok := config.Queues[testQueue2]; !ok || !q.AutoDelete {
		t.Errorf("%s not recorded correctly", testQueue2)
	}

	b1 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1}
	b2 := BindingConfig{Queue: testQueue2, Exchange: testExchange1, Key: testKey2}
	b3 := BindingConfig{Queue: testQueue1, Exchange: testExchange2, Key: testKey3}

	ch.recordBinding(b1)
	ch.recordBinding(b2)
	ch.recordBinding(b3)

	config = ch.TopologyConfiguration()
	if len(config.Bindings) != 3 {
		t.Fatalf("expected 3 bindings, got %d", len(config.Bindings))
	}

	ch.removeQueue(testQueue1)

	config = ch.TopologyConfiguration()
	if _, ok := config.Queues[testQueue1]; ok {
		t.Errorf("%s should be deleted from Queues", testQueue1)
	}
	if _, ok := config.Queues[testQueue2]; !ok {
		t.Errorf("%s should not be deleted from Queues", testQueue2)
	}

	if len(config.Bindings) != 1 {
		t.Fatalf("expected 1 remaining binding, got %d", len(config.Bindings))
	}
	if !compareBinding(config.Bindings[0], b2) {
		t.Errorf("expected remaining binding to be %+v, got %+v", b2, config.Bindings[0])
	}

	// Verify that removing a queue from a channel with uninitialized (nil)
	// topology collections is a safe no-op and does not cause a panic.
	chNil := &Channel{}
	chNil.removeQueue("any")
}

func TestRecordAndRemoveExchange(t *testing.T) {
	ch := &Channel{}

	ec1 := ExchangeConfig{Name: testExchange1, Kind: testExchangeKindDirect}
	ec2 := ExchangeConfig{Name: testExchange2, Kind: testExchangeKindTopic}

	ch.recordExchange(ec1)
	ch.recordExchange(ec2)

	config := ch.TopologyConfiguration()
	if len(config.Exchanges) != 2 {
		t.Fatalf("expected 2 exchanges, got %d", len(config.Exchanges))
	}
	if ex, ok := config.Exchanges[testExchange1]; !ok || ex.Kind != testExchangeKindDirect {
		t.Errorf("%s not recorded correctly", testExchange1)
	}
	if ex, ok := config.Exchanges[testExchange2]; !ok || ex.Kind != testExchangeKindTopic {
		t.Errorf("%s not recorded correctly", testExchange2)
	}

	b1 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1}
	b2 := BindingConfig{Queue: testQueue2, Exchange: testExchange2, Key: testKey2}
	b3 := BindingConfig{Queue: testQueue3, Exchange: testExchange1, Key: testKey3}

	ch.recordBinding(b1)
	ch.recordBinding(b2)
	ch.recordBinding(b3)

	eb1 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKeyEb1}
	eb2 := ExchangeBindingConfig{Source: testExchange2, Destination: testExchange3, Key: testKeyEb2}
	eb3 := ExchangeBindingConfig{Source: testExchange3, Destination: testExchange1, Key: testKeyEb3}
	eb4 := ExchangeBindingConfig{Source: testExchange3, Destination: testExchange4, Key: testKeyEb4}

	ch.recordExchangeBinding(eb1)
	ch.recordExchangeBinding(eb2)
	ch.recordExchangeBinding(eb3)
	ch.recordExchangeBinding(eb4)

	ch.removeExchange(testExchange1)

	config = ch.TopologyConfiguration()
	if _, ok := config.Exchanges[testExchange1]; ok {
		t.Errorf("%s should be deleted from Exchanges", testExchange1)
	}
	if _, ok := config.Exchanges[testExchange2]; !ok {
		t.Errorf("%s should not be deleted from Exchanges", testExchange2)
	}

	if len(config.Bindings) != 1 {
		t.Fatalf("expected 1 remaining queue binding, got %d", len(config.Bindings))
	}
	if !compareBinding(config.Bindings[0], b2) {
		t.Errorf("expected remaining queue binding to be %+v, got %+v", b2, config.Bindings[0])
	}

	if len(config.ExchangeBindings) != 2 {
		t.Fatalf("expected 2 remaining exchange bindings, got %d", len(config.ExchangeBindings))
	}
	for _, eb := range config.ExchangeBindings {
		if eb.Source == testExchange1 || eb.Destination == testExchange1 {
			t.Errorf("found exchange binding containing %s: %+v", testExchange1, eb)
		}
	}

	// Verify that removing an exchange from a channel with uninitialized (nil)
	// topology collections is a safe no-op and does not cause a panic.
	chNil := &Channel{}
	chNil.removeExchange("any")
}

func TestRecordAndRemoveBinding(t *testing.T) {
	ch := &Channel{}

	b1 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1}
	b2 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey2}
	b3 := BindingConfig{Queue: testQueue2, Exchange: testExchange1, Key: testKey1}
	b4 := BindingConfig{Queue: testQueue1, Exchange: testExchange2, Key: testKey1}

	ch.recordBinding(b1)
	ch.recordBinding(b2)
	ch.recordBinding(b3)
	ch.recordBinding(b4)

	config := ch.TopologyConfiguration()
	if len(config.Bindings) != 4 {
		t.Fatalf("expected 4 bindings, got %d", len(config.Bindings))
	}

	ch.removeBinding(b1)

	config = ch.TopologyConfiguration()
	if len(config.Bindings) != 3 {
		t.Fatalf("expected 3 remaining bindings, got %d", len(config.Bindings))
	}

	foundB2, foundB3, foundB4 := false, false, false
	for _, b := range config.Bindings {
		if compareBinding(b, b2) {
			foundB2 = true
		} else if compareBinding(b, b3) {
			foundB3 = true
		} else if compareBinding(b, b4) {
			foundB4 = true
		} else if compareBinding(b, b1) {
			t.Error("b1 was not removed")
		}
	}

	if !foundB2 || !foundB3 || !foundB4 {
		t.Errorf("expected b2, b3, b4 to remain, but some were missing. Active: %+v", config.Bindings)
	}

	// For in-place compaction check, we must inspect the unexported channel's
	// internal slice since TopologyConfiguration() returns a clone.
	underlyingSlice := ch.topologyConfiguration.Bindings[:cap(ch.topologyConfiguration.Bindings)]
	if underlyingSlice[3].Queue != "" {
		t.Error("expected compacted slots in the underlying slice to be zero-valued")
	}

	ch.removeBinding(BindingConfig{Queue: "non-existent", Exchange: "non-existent", Key: "non-existent"})
	config = ch.TopologyConfiguration()
	if len(config.Bindings) != 3 {
		t.Fatalf("expected 3 bindings after non-existent unbind, got %d", len(config.Bindings))
	}
}

func TestRecordAndRemoveExchangeBinding(t *testing.T) {
	ch := &Channel{}

	eb1 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey1}
	eb2 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey2}
	eb3 := ExchangeBindingConfig{Source: testExchange2, Destination: testExchange3, Key: testKey1}
	eb4 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange3, Key: testKey1}

	ch.recordExchangeBinding(eb1)
	ch.recordExchangeBinding(eb2)
	ch.recordExchangeBinding(eb3)
	ch.recordExchangeBinding(eb4)

	config := ch.TopologyConfiguration()
	if len(config.ExchangeBindings) != 4 {
		t.Fatalf("expected 4 exchange bindings, got %d", len(config.ExchangeBindings))
	}

	ch.removeExchangeBinding(eb1)

	config = ch.TopologyConfiguration()
	if len(config.ExchangeBindings) != 3 {
		t.Fatalf("expected 3 remaining exchange bindings, got %d", len(config.ExchangeBindings))
	}

	foundEB2, foundEB3, foundEB4 := false, false, false
	for _, eb := range config.ExchangeBindings {
		if compareExchangeBinding(eb, eb2) {
			foundEB2 = true
		} else if compareExchangeBinding(eb, eb3) {
			foundEB3 = true
		} else if compareExchangeBinding(eb, eb4) {
			foundEB4 = true
		} else if compareExchangeBinding(eb, eb1) {
			t.Error("eb1 was not removed")
		}
	}

	if !foundEB2 || !foundEB3 || !foundEB4 {
		t.Errorf("expected eb2, eb3, eb4 to remain, but some were missing. Active: %+v", config.ExchangeBindings)
	}

	// For in-place compaction check, we must inspect the unexported channel's
	// internal slice since TopologyConfiguration() returns a clone.
	underlyingSlice := ch.topologyConfiguration.ExchangeBindings[:cap(ch.topologyConfiguration.ExchangeBindings)]
	if underlyingSlice[3].Source != "" {
		t.Error("expected compacted slots in the underlying slice to be zero-valued")
	}

	ch.removeExchangeBinding(ExchangeBindingConfig{Source: "non-existent", Destination: "non-existent", Key: "non-existent"})
	config = ch.TopologyConfiguration()
	if len(config.ExchangeBindings) != 3 {
		t.Fatalf("expected 3 exchange bindings after non-existent unbind, got %d", len(config.ExchangeBindings))
	}
}
