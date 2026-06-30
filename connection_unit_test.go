// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"crypto/tls"
	"errors"
	"net"
	"os"
	"path/filepath"
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

func TestTLSConfigFromURIEnforcesMinVersion(t *testing.T) {
	cfg, err := tlsConfigFromURI(URI{
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("failed to create TLS config from URI: %v", err)
	}

	if cfg == nil {
		t.Fatal("expected tls.Config, got nil")
	}

	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected MinVersion TLS 1.2 (%d), got %d", tls.VersionTLS12, cfg.MinVersion)
	}
}

func TestTLSConfigFromURIWithCACertAndClientCerts(t *testing.T) {
	tempDir := t.TempDir()

	caPath := filepath.Join(tempDir, "ca.pem")
	certPath := filepath.Join(tempDir, "client.pem")
	keyPath := filepath.Join(tempDir, "client-key.pem")

	if err := os.WriteFile(caPath, []byte(caCert), 0600); err != nil {
		t.Fatalf("failed to write CA cert: %v", err)
	}
	defer func() { _ = os.Remove(caPath) }()

	if err := os.WriteFile(certPath, []byte(clientCert), 0600); err != nil {
		t.Fatalf("failed to write client cert: %v", err)
	}
	defer func() { _ = os.Remove(certPath) }()

	if err := os.WriteFile(keyPath, []byte(clientKey), 0600); err != nil {
		t.Fatalf("failed to write client key: %v", err)
	}
	defer func() { _ = os.Remove(keyPath) }()

	cfg, err := tlsConfigFromURI(URI{
		CACertFile: caPath,
		CertFile:   certPath,
		KeyFile:    keyPath,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("failed to create TLS config from URI with CA and client certs: %v", err)
	}

	if cfg == nil {
		t.Fatal("expected tls.Config, got nil")
	}

	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected MinVersion TLS 1.2 (%d), got %d", tls.VersionTLS12, cfg.MinVersion)
	}

	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cfg.Certificates))
	}

	if cfg.RootCAs == nil {
		t.Fatal("expected RootCAs to be set, got nil")
	}

	if cfg.ServerName != "localhost" {
		t.Fatalf("expected ServerName localhost, got %q", cfg.ServerName)
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
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}
	ch := &Channel{
		connection: conn,
		consumers:  makeConsumers(),
	}
	defer ch.consumers.close()

	ch.consumers.add("ctag1", make(chan Delivery), consumerConfig{Queue: testQueue1})
	ch.consumers.add("ctag2", make(chan Delivery), consumerConfig{Queue: testQueue2})

	qc1 := QueueConfig{ActualName: testQueue1, Durable: true}
	qc2 := QueueConfig{ActualName: testQueue2, AutoDelete: true}

	conn.recordQueue(ch.id, qc1)
	conn.recordQueue(ch.id, qc2)

	config := conn.getTopologyConfiguration(ch.id, false)
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

	conn.recordBinding(ch.id, b1)
	conn.recordBinding(ch.id, b2)
	conn.recordBinding(ch.id, b3)

	config = conn.getTopologyConfiguration(ch.id, false)
	if len(config.Bindings) != 3 {
		t.Fatalf("expected 3 bindings, got %d", len(config.Bindings))
	}

	conn.removeQueue(testQueue1)
	ch.consumers.cancelByQueue(testQueue1)

	ch.consumers.Lock()
	_, found1 := ch.consumers.configs["ctag1"]
	_, found2 := ch.consumers.configs["ctag2"]
	ch.consumers.Unlock()

	if found1 {
		t.Error("Expected consumer for removed queue testQueue1 to be cancelled/removed")
	}
	if !found2 {
		t.Error("Expected consumer for remaining queue testQueue2 to still exist")
	}

	config = conn.getTopologyConfiguration(ch.id, false)
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

	// Verify that removing a queue from a connection with nil topology is a safe no-op.
	connEmpty := &Connection{}
	connEmpty.removeQueue("any")
}

func TestRecordAndRemoveExchange(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}

	ec1 := ExchangeConfig{Name: testExchange1, Kind: testExchangeKindDirect}
	ec2 := ExchangeConfig{Name: testExchange2, Kind: testExchangeKindTopic}

	conn.recordExchange(0, ec1)
	conn.recordExchange(0, ec2)

	config := conn.getTopologyConfiguration(0, false)
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

	conn.recordBinding(0, b1)
	conn.recordBinding(0, b2)
	conn.recordBinding(0, b3)

	eb1 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKeyEb1}
	eb2 := ExchangeBindingConfig{Source: testExchange2, Destination: testExchange3, Key: testKeyEb2}
	eb3 := ExchangeBindingConfig{Source: testExchange3, Destination: testExchange1, Key: testKeyEb3}
	eb4 := ExchangeBindingConfig{Source: testExchange3, Destination: testExchange4, Key: testKeyEb4}

	conn.recordExchangeBinding(0, eb1)
	conn.recordExchangeBinding(0, eb2)
	conn.recordExchangeBinding(0, eb3)
	conn.recordExchangeBinding(0, eb4)

	conn.removeExchange(testExchange1)

	config = conn.getTopologyConfiguration(0, false)
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

	// Verify that removing an exchange from a connection with nil topology is a safe no-op.
	connEmpty := &Connection{}
	connEmpty.removeExchange("any")
}

func TestRecordAndRemoveBinding(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}

	b1 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1}
	b2 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey2}
	b3 := BindingConfig{Queue: testQueue2, Exchange: testExchange1, Key: testKey1}
	b4 := BindingConfig{Queue: testQueue1, Exchange: testExchange2, Key: testKey1}

	conn.recordBinding(0, b1)
	conn.recordBinding(0, b2)
	conn.recordBinding(0, b3)
	conn.recordBinding(0, b4)

	config := conn.getTopologyConfiguration(0, false)
	if len(config.Bindings) != 4 {
		t.Fatalf("expected 4 bindings, got %d", len(config.Bindings))
	}

	conn.removeBinding(b1)

	config = conn.getTopologyConfiguration(0, false)
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

	// Verify in-place compaction zeroed the vacated slot in the internal slice.
	internalSlice := conn.topologyConfiguration[0].Bindings
	if internalSlice[:cap(internalSlice)][3].Queue != "" {
		t.Error("expected compacted slots in the underlying slice to be zero-valued")
	}

	conn.removeBinding(BindingConfig{Queue: "non-existent", Exchange: "non-existent", Key: "non-existent"})
	config = conn.getTopologyConfiguration(0, false)
	if len(config.Bindings) != 3 {
		t.Fatalf("expected 3 bindings after non-existent unbind, got %d", len(config.Bindings))
	}
}

func TestRecordAndRemoveExchangeBinding(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}

	eb1 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey1}
	eb2 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey2}
	eb3 := ExchangeBindingConfig{Source: testExchange2, Destination: testExchange3, Key: testKey1}
	eb4 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange3, Key: testKey1}

	conn.recordExchangeBinding(0, eb1)
	conn.recordExchangeBinding(0, eb2)
	conn.recordExchangeBinding(0, eb3)
	conn.recordExchangeBinding(0, eb4)

	config := conn.getTopologyConfiguration(0, false)
	if len(config.ExchangeBindings) != 4 {
		t.Fatalf("expected 4 exchange bindings, got %d", len(config.ExchangeBindings))
	}

	conn.removeExchangeBinding(eb1)

	config = conn.getTopologyConfiguration(0, false)
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

	// Verify in-place compaction zeroed the vacated slot in the internal slice.
	internalSlice := conn.topologyConfiguration[0].ExchangeBindings
	if internalSlice[:cap(internalSlice)][3].Source != "" {
		t.Error("expected compacted slots in the underlying slice to be zero-valued")
	}

	conn.removeExchangeBinding(ExchangeBindingConfig{Source: "non-existent", Destination: "non-existent", Key: "non-existent"})
	config = conn.getTopologyConfiguration(0, false)
	if len(config.ExchangeBindings) != 3 {
		t.Fatalf("expected 3 exchange bindings after non-existent unbind, got %d", len(config.ExchangeBindings))
	}
}

func TestRecordBindingDeduplication(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}

	b1 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1}
	b2 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1} // duplicate
	b3 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1, Args: Table{"foo": "bar"}}
	b4 := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1, Args: Table{"foo": "bar"}} // duplicate of b3

	conn.recordBinding(0, b1)
	conn.recordBinding(0, b2)
	conn.recordBinding(0, b3)
	conn.recordBinding(0, b4)

	config := conn.getTopologyConfiguration(0, false)
	if len(config.Bindings) != 2 {
		t.Fatalf("expected 2 unique bindings, got %d", len(config.Bindings))
	}

	eb1 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey1}
	eb2 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey1} // duplicate
	eb3 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey1, Args: Table{"foo": "bar"}}
	eb4 := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey1, Args: Table{"foo": "bar"}} // duplicate of eb3

	conn.recordExchangeBinding(0, eb1)
	conn.recordExchangeBinding(0, eb2)
	conn.recordExchangeBinding(0, eb3)
	conn.recordExchangeBinding(0, eb4)

	config = conn.getTopologyConfiguration(0, false)
	if len(config.ExchangeBindings) != 2 {
		t.Fatalf("expected 2 unique exchange bindings, got %d", len(config.ExchangeBindings))
	}
}

// --- getTopologyConfiguration tests ---

func isEmptyTopology(config TopologyConfiguration) bool {
	return len(config.Exchanges) == 0 && len(config.Queues) == 0 &&
		len(config.Bindings) == 0 && len(config.ExchangeBindings) == 0 && config.Qos == nil
}

func TestGetTopologyConfigurationNilMap(t *testing.T) {
	conn := &Connection{} // topologyConfiguration is nil
	if !isEmptyTopology(conn.getTopologyConfiguration(1, false)) {
		t.Errorf("expected empty topology for nil map (local)")
	}
	if !isEmptyTopology(conn.getTopologyConfiguration(1, true)) {
		t.Errorf("expected empty topology for nil map (global)")
	}
}

func TestGetTopologyConfigurationChannelLocalUnknownChannel(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}
	if !isEmptyTopology(conn.getTopologyConfiguration(99, false)) {
		t.Errorf("expected empty topology for unknown channel")
	}
}

func TestGetTopologyConfigurationChannelLocalView(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}

	conn.recordExchange(1, ExchangeConfig{Name: testExchange1, Kind: testExchangeKindDirect})
	conn.recordQueue(1, QueueConfig{ActualName: testQueue1, Durable: true})
	conn.recordBinding(1, BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1})

	conn.recordExchange(2, ExchangeConfig{Name: testExchange2, Kind: testExchangeKindTopic})
	conn.recordQueue(2, QueueConfig{ActualName: testQueue2})

	config := conn.getTopologyConfiguration(1, false)
	if len(config.Exchanges) != 1 {
		t.Fatalf("expected 1 exchange on channel 1, got %d", len(config.Exchanges))
	}
	if _, ok := config.Exchanges[testExchange1]; !ok {
		t.Errorf("expected %s on channel 1", testExchange1)
	}
	if _, ok := config.Exchanges[testExchange2]; ok {
		t.Errorf("did not expect %s on channel 1", testExchange2)
	}
	if len(config.Queues) != 1 {
		t.Fatalf("expected 1 queue on channel 1, got %d", len(config.Queues))
	}
	if len(config.Bindings) != 1 {
		t.Fatalf("expected 1 binding on channel 1, got %d", len(config.Bindings))
	}
}

func TestGetTopologyConfigurationGlobalMerge(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}

	conn.recordExchange(1, ExchangeConfig{Name: testExchange1, Kind: testExchangeKindDirect})
	conn.recordQueue(1, QueueConfig{ActualName: testQueue1, Durable: true})
	conn.recordBinding(1, BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1})

	conn.recordExchange(2, ExchangeConfig{Name: testExchange2, Kind: testExchangeKindTopic})
	conn.recordQueue(2, QueueConfig{ActualName: testQueue2})
	conn.recordBinding(2, BindingConfig{Queue: testQueue2, Exchange: testExchange2, Key: testKey2})

	config := conn.getTopologyConfiguration(1, true)
	if len(config.Exchanges) != 2 {
		t.Fatalf("expected 2 exchanges in global view, got %d", len(config.Exchanges))
	}
	if len(config.Queues) != 2 {
		t.Fatalf("expected 2 queues in global view, got %d", len(config.Queues))
	}
	if len(config.Bindings) != 2 {
		t.Fatalf("expected 2 bindings in global view, got %d", len(config.Bindings))
	}
}

func TestGetTopologyConfigurationGlobalDeduplicatesBindings(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}

	// Same binding recorded on two different channels.
	b := BindingConfig{Queue: testQueue1, Exchange: testExchange1, Key: testKey1}
	conn.recordBinding(1, b)
	conn.recordBinding(2, b)

	eb := ExchangeBindingConfig{Source: testExchange1, Destination: testExchange2, Key: testKey1}
	conn.recordExchangeBinding(1, eb)
	conn.recordExchangeBinding(2, eb)

	config := conn.getTopologyConfiguration(1, true)
	if len(config.Bindings) != 1 {
		t.Fatalf("expected 1 deduplicated binding in global view, got %d", len(config.Bindings))
	}
	if len(config.ExchangeBindings) != 1 {
		t.Fatalf("expected 1 deduplicated exchange binding in global view, got %d", len(config.ExchangeBindings))
	}
}

func TestGetTopologyConfigurationQoSIsChannelScoped(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}

	conn.topologyConfiguration[1] = newTopologyConfiguration()
	conn.topologyConfiguration[1].Qos = &QosConfig{PrefetchCount: 10}
	conn.topologyConfiguration[2] = newTopologyConfiguration()
	conn.topologyConfiguration[2].Qos = &QosConfig{PrefetchCount: 20}

	// Channel-local: should return channel 1's QoS.
	local := conn.getTopologyConfiguration(1, false)
	if local.Qos == nil || local.Qos.PrefetchCount != 10 {
		t.Errorf("expected channel 1 QoS prefetch=10, got %+v", local.Qos)
	}

	// Global merge should still return channel 1's QoS, not channel 2's.
	global := conn.getTopologyConfiguration(1, true)
	if global.Qos == nil || global.Qos.PrefetchCount != 10 {
		t.Errorf("expected global view to carry channel 1 QoS prefetch=10, got %+v", global.Qos)
	}

	// Global merge for channel 2 should return channel 2's QoS.
	global2 := conn.getTopologyConfiguration(2, true)
	if global2.Qos == nil || global2.Qos.PrefetchCount != 20 {
		t.Errorf("expected global view to carry channel 2 QoS prefetch=20, got %+v", global2.Qos)
	}
}

func TestGetTopologyConfigurationQoSMissingChannel(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}
	conn.topologyConfiguration[1] = newTopologyConfiguration()
	// No QoS set on channel 1; requesting channel 99 (absent).
	config := conn.getTopologyConfiguration(99, false)
	if config.Qos != nil {
		t.Errorf("expected nil QoS for absent channel, got %+v", config.Qos)
	}
}

func TestGetTopologyConfigurationReturnIsSnapshot(t *testing.T) {
	conn := &Connection{
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
	}
	conn.recordExchange(1, ExchangeConfig{Name: testExchange1, Kind: testExchangeKindDirect})

	config := conn.getTopologyConfiguration(1, false)
	// Mutate the returned snapshot; the stored topology must not change.
	config.Exchanges[testExchange2] = ExchangeConfig{Name: testExchange2}

	stored := conn.getTopologyConfiguration(1, false)
	if _, ok := stored.Exchanges[testExchange2]; ok {
		t.Error("mutating returned snapshot should not affect stored topology")
	}
}

// --- filterTransientTopology tests ---

func TestFilterTransientTopologyAllDurable(t *testing.T) {
	exchanges := map[string]ExchangeConfig{
		testExchange1: {Name: testExchange1, AutoDelete: false},
	}
	queues := map[string]QueueConfig{
		testQueue1: {ActualName: testQueue1, AutoDelete: false, Exclusive: false},
	}
	bindings := []BindingConfig{
		{Queue: testQueue1, Exchange: testExchange1, Key: testKey1},
	}
	exchangeBindings := []ExchangeBindingConfig{
		{Source: testExchange1, Destination: testExchange2, Key: testKey1},
	}

	fEx, fQ, fB, fEB := filterTransientTopology(
		exchanges, queues, bindings, exchangeBindings,
		map[string]bool{}, map[string]bool{},
	)
	if len(fEx) != 0 {
		t.Errorf("expected 0 filtered exchanges (all durable), got %d", len(fEx))
	}
	if len(fQ) != 0 {
		t.Errorf("expected 0 filtered queues (all durable), got %d", len(fQ))
	}
	if len(fB) != 0 {
		t.Errorf("expected 0 filtered bindings, got %d", len(fB))
	}
	if len(fEB) != 0 {
		t.Errorf("expected 0 filtered exchange bindings, got %d", len(fEB))
	}
}

func TestFilterTransientTopologyAutoDeleteExchange(t *testing.T) {
	exchanges := map[string]ExchangeConfig{
		testExchange1: {Name: testExchange1, AutoDelete: true},
		testExchange2: {Name: testExchange2, AutoDelete: false},
	}

	fEx, _, _, _ := filterTransientTopology(
		exchanges, nil, nil, nil,
		map[string]bool{}, map[string]bool{},
	)
	if len(fEx) != 1 {
		t.Fatalf("expected 1 auto-delete exchange, got %d", len(fEx))
	}
	if _, ok := fEx[testExchange1]; !ok {
		t.Errorf("expected %s (auto-delete) to be retained", testExchange1)
	}
}

func TestFilterTransientTopologyExclusiveAndAutoDeleteQueues(t *testing.T) {
	queues := map[string]QueueConfig{
		testQueue1: {ActualName: testQueue1, Exclusive: true},
		testQueue2: {ActualName: testQueue2, AutoDelete: true},
		testQueue3: {ActualName: testQueue3, Durable: true},
	}

	_, fQ, _, _ := filterTransientTopology(
		nil, queues, nil, nil,
		map[string]bool{}, map[string]bool{},
	)
	if len(fQ) != 2 {
		t.Fatalf("expected 2 transient queues, got %d", len(fQ))
	}
	if _, ok := fQ[testQueue1]; !ok {
		t.Errorf("expected exclusive %s to be retained", testQueue1)
	}
	if _, ok := fQ[testQueue2]; !ok {
		t.Errorf("expected auto-delete %s to be retained", testQueue2)
	}
	if _, ok := fQ[testQueue3]; ok {
		t.Errorf("expected durable %s to be filtered out", testQueue3)
	}
}

func TestFilterTransientTopologyBindingsRetainedByTransientQueueOrExchange(t *testing.T) {
	bindings := []BindingConfig{
		// retained: queue is transient
		{Queue: testQueue1, Exchange: testExchange1, Key: testKey1},
		// retained: exchange is transient
		{Queue: testQueue2, Exchange: testExchange2, Key: testKey2},
		// dropped: both are durable
		{Queue: testQueue3, Exchange: testExchange3, Key: testKey3},
	}

	globalTransientQueues := map[string]bool{testQueue1: true}
	globalTransientExchanges := map[string]bool{testExchange2: true}

	_, _, fB, _ := filterTransientTopology(
		nil, nil, bindings, nil,
		globalTransientQueues, globalTransientExchanges,
	)
	if len(fB) != 2 {
		t.Fatalf("expected 2 retained bindings, got %d: %+v", len(fB), fB)
	}
	for _, b := range fB {
		if b.Queue == testQueue3 && b.Exchange == testExchange3 {
			t.Errorf("binding between two durable entities should have been filtered: %+v", b)
		}
	}
}

func TestFilterTransientTopologyExchangeBindingsRetainedByTransientSourceOrDest(t *testing.T) {
	exchangeBindings := []ExchangeBindingConfig{
		// retained: source is transient
		{Source: testExchange1, Destination: testExchange2, Key: testKeyEb1},
		// retained: destination is transient
		{Source: testExchange3, Destination: testExchange4, Key: testKeyEb2},
		// dropped: both are durable
		{Source: testExchange2, Destination: testExchange3, Key: testKeyEb3},
	}

	globalTransientExchanges := map[string]bool{testExchange1: true, testExchange4: true}

	_, _, _, fEB := filterTransientTopology(
		nil, nil, nil, exchangeBindings,
		map[string]bool{}, globalTransientExchanges,
	)
	if len(fEB) != 2 {
		t.Fatalf("expected 2 retained exchange bindings, got %d: %+v", len(fEB), fEB)
	}
	for _, eb := range fEB {
		if eb.Source == testExchange2 && eb.Destination == testExchange3 {
			t.Errorf("exchange binding between two durable exchanges should have been filtered: %+v", eb)
		}
	}
}

func TestFilterTransientTopologyMixedScenario(t *testing.T) {
	exchanges := map[string]ExchangeConfig{
		testExchange1: {Name: testExchange1, AutoDelete: true},
		testExchange2: {Name: testExchange2, AutoDelete: false},
	}
	queues := map[string]QueueConfig{
		testQueue1: {ActualName: testQueue1, Exclusive: true},
		testQueue2: {ActualName: testQueue2, Durable: true},
	}
	bindings := []BindingConfig{
		{Queue: testQueue1, Exchange: testExchange2, Key: testKey1}, // retained: queue is transient
		{Queue: testQueue2, Exchange: testExchange1, Key: testKey2}, // retained: exchange is transient
		{Queue: testQueue2, Exchange: testExchange2, Key: testKey3}, // dropped
	}
	exchangeBindings := []ExchangeBindingConfig{
		{Source: testExchange1, Destination: testExchange2, Key: testKeyEb1}, // retained: source transient
		{Source: testExchange2, Destination: testExchange2, Key: testKeyEb2}, // dropped
	}

	globalTransientQueues := map[string]bool{testQueue1: true}
	globalTransientExchanges := map[string]bool{testExchange1: true}

	fEx, fQ, fB, fEB := filterTransientTopology(
		exchanges, queues, bindings, exchangeBindings,
		globalTransientQueues, globalTransientExchanges,
	)

	if len(fEx) != 1 {
		t.Fatalf("expected 1 filtered exchange, got %d", len(fEx))
	}
	if _, ok := fEx[testExchange1]; !ok {
		t.Errorf("expected auto-delete %s to be retained", testExchange1)
	}

	if len(fQ) != 1 {
		t.Fatalf("expected 1 filtered queue, got %d", len(fQ))
	}
	if _, ok := fQ[testQueue1]; !ok {
		t.Errorf("expected exclusive %s to be retained", testQueue1)
	}

	if len(fB) != 2 {
		t.Fatalf("expected 2 filtered bindings, got %d: %+v", len(fB), fB)
	}

	if len(fEB) != 1 {
		t.Fatalf("expected 1 filtered exchange binding, got %d: %+v", len(fEB), fEB)
	}
}
