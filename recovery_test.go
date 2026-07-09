// Copyright (c) 2026 Broadcom. All Rights Reserved.
// The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

//go:build integration

package amqp091

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go/internal/utils"
)

// TestConnectionRecoveryPublish tests the connection recovery for publish.
func TestConnectionRecoveryPublish(t *testing.T) {
	connectionName := "test-connection-recovery-publish"
	// Create a connection with Recovery
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	exchangeName := "recovery_exchange"
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		false,        // durable
		true,         // auto-delete
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}
	defer func() {
		_ = ch.ExchangeDelete(exchangeName, false, false)
	}()

	queueName := "recovery_publish_queue"
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // auto-delete
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}
	defer func() {
		_, _ = ch.QueueDelete(queueName, false, false, false)
	}()

	routingKey := "recovery_routing_key"
	err = ch.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	// Publish message on the given channel
	preRecoveryMessage := "hello recovery 1"
	err = ch.PublishWithContext(
		context.Background(),
		exchangeName,
		routingKey,
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte(preRecoveryMessage),
		},
	)
	if err != nil {
		t.Fatalf("Publish pre-recovery message failed: %v", err)
	}
	t.Logf("Published message pre-recovery: %s", preRecoveryMessage)

	// Consume message on the given channel
	msgs, err := ch.Consume(
		queueName,
		"recovery_publish_consumer", // consumer tag
		true,                        // autoAck
		false,                       // exclusive
		false,                       // noLocal
		false,                       // noWait
		nil,                         // args
	)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	select {
	case d, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed prematurely")
		}
		if string(d.Body) != preRecoveryMessage {
			t.Fatalf("Expected message '%s', got: %s", preRecoveryMessage, string(d.Body))
		}
		t.Logf("Received message pre-recovery: %s", string(d.Body))
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for receive message pre-recovery: %s", preRecoveryMessage)
	}

	// Register with connection for NotifyStateChange
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)

	// Register with channel for NotifyStateChange
	chanStateChanged := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateChanged)

	// Call Http API to close the current connection
	dropConnection(t, connectionName)

	// Wait for connection to be open
	waitForConnectionOpen(t, stateChanged)

	// Verify channel state change notification is received and is reconnecting, followed by open
	waitForChannelOpen(t, chanStateChanged)

	// Verify Publish message on the given channel post-recovery.
	postRecoveryMessage := "hello recovery 2"
	err = ch.PublishWithContext(
		context.Background(),
		exchangeName,
		routingKey,
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte(postRecoveryMessage),
		},
	)
	if err != nil {
		t.Fatalf("Publish post-recovery message failed: %v", err)
	}
	t.Logf("Published message post-recovery: %s", postRecoveryMessage)

	// Verify message is received on the given channel post-recovery.
	select {
	case d, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed after recovery")
		}
		if string(d.Body) != postRecoveryMessage {
			t.Fatalf("Expected message '%s', got: %s", postRecoveryMessage, string(d.Body))
		}
		t.Logf("Received message post-recovery: %s", string(d.Body))
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for receive message post-recovery: %s", postRecoveryMessage)
	}
}

// TestConnectionRecoveryConsume tests the connection recovery for consume.
func TestConnectionRecoveryConsume(t *testing.T) {
	connectionName := "test-connection-recovery-consume"
	// Create a connection with Recovery
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	queueName := "recovery_consume_test_queue"
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // auto-delete
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}
	defer func() {
		_, _ = ch.QueueDelete(queueName, false, false, false)
	}()

	// Create Consumer with auto-ack false
	msgs, err := ch.Consume(
		queueName,
		"consume-recovery-test",
		false, // autoAck = false
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Publish a message on the channel.
	err = ch.PublishWithContext(
		context.Background(),
		"",        // exchange
		queueName, // routing key = queue name
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello recovery consume"),
		},
	)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume message and do not send ack.
	select {
	case msg, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed prematurely")
		}
		if string(msg.Body) != "hello recovery consume" {
			t.Fatalf("Expected message 'hello recovery consume', got: %s", string(msg.Body))
		}
		t.Logf("Received message pre-recovery: %s (Redelivered: %t). Intentional no ACK.", string(msg.Body), msg.Redelivered)
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for message delivery pre-recovery")
	}

	// Register with connection for NotifyStateChange
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)

	// Register with channel for NotifyStateChange
	chanStateChanged := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateChanged)

	// Drop the connection
	dropConnection(t, connectionName)

	// Wait for connection to recover using connection.NotifyStateChange like before
	waitForConnectionOpen(t, stateChanged)

	// Wait for channel to recover using channel.NotifyStateChange like before
	waitForChannelOpen(t, chanStateChanged)

	// Confirm original message is received by the consumer and ack true.
	select {
	case msg, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed after recovery")
		}
		if string(msg.Body) != "hello recovery consume" {
			t.Fatalf("Expected message 'hello recovery consume', got: %s", string(msg.Body))
		}
		t.Logf("Received message post-recovery: %s (Redelivered: %t). Sending ACK.", string(msg.Body), msg.Redelivered)

		err = msg.Ack(false)
		if err != nil {
			t.Fatalf("Acking redelivered message post-recovery failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for message redelivery post-recovery")
	}
}

func dropConnection(t *testing.T, name string) {
	var targetConnName string
	loopDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(loopDeadline) {
		connection, err := utils.GetConnectionByName(name)
		if err != nil {
			t.Logf("Failure getting connection by name (will retry): %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		targetConnName = connection.Name
		break
	}

	if targetConnName == "" {
		conns, _ := utils.Connections()
		t.Fatalf("Could not find connection by name: %s in connections: %+v", name, conns)
	}

	t.Logf("Dropping connection: %s", targetConnName)
	err := utils.DropConnection(url.PathEscape(targetConnName), "15672")
	if err != nil {
		t.Fatalf("DropConnection failed: %v", err)
	}
}

func waitForConnectionOpen(t *testing.T, stateChanged chan *StateChanged) {
	var connReconnectingSeen bool
	var connOpenSeen bool
	for !connOpenSeen {
		select {
		case sc := <-stateChanged:
			t.Logf("Connection state changed: %s", sc)
			if sc.To == StateReconnecting {
				connReconnectingSeen = true
			}
			if sc.To == StateOpen {
				connOpenSeen = true
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for connection recovery state changes. Reconnecting seen: %t, Open seen: %t", connReconnectingSeen, connOpenSeen)
		}
	}
}

func waitForChannelOpen(t *testing.T, chanStateChanged chan *StateChanged) {
	var chanReconnectingSeen bool
	var chanOpenSeen bool

	for !chanOpenSeen {
		select {
		case sc := <-chanStateChanged:
			t.Logf("Channel state changed: %s", sc)
			if sc.To == StateReconnecting {
				chanReconnectingSeen = true
			}
			if sc.To == StateOpen {
				chanOpenSeen = true
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for channel recovery state changes. Reconnecting seen: %t, Open seen: %t", chanReconnectingSeen, chanOpenSeen)
		}
	}
}

func waitForStateChangeClose(t *testing.T, ch chan *StateChanged, name, listenerName string) {
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
		t.Logf("State change channel for %s %s cleanly closed", name, listenerName)
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for state change channel for %s %s to close", name, listenerName)
	}
}

// TestConnectionRecoveryChannelIDReservation verifies that after connection recovery,
// the allocator correctly reserves the IDs of recovered channels, and subsequently
// opened channels do not conflict with the recovered channel's ID.
func TestConnectionRecoveryChannelIDReservation(t *testing.T) {
	connectionName := "test-channel-id-reservation-recovery"
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	// Open channels 1, 2, 3, 4
	ch1, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 1 creation failed: %v", err)
	}
	defer ch1.Close()

	ch2, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 2 creation failed: %v", err)
	}
	defer ch2.Close()

	ch3, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 3 creation failed: %v", err)
	}
	defer ch3.Close()

	ch4, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 4 creation failed: %v", err)
	}
	defer ch4.Close()

	id1, id2, id3, id4 := ch1.id, ch2.id, ch3.id, ch4.id
	t.Logf("Opened channels with IDs: %d, %d, %d, %d", id1, id2, id3, id4)

	// Close channels 2 and 4
	if err := ch2.Close(); err != nil {
		t.Fatalf("Failed to close channel 2: %v", err)
	}
	if err := ch4.Close(); err != nil {
		t.Fatalf("Failed to close channel 4: %v", err)
	}
	t.Logf("Closed channels with IDs: %d, %d", id2, id4)

	// Register for NotifyStateChange
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)

	chanStateChanged1 := make(chan *StateChanged, 10)
	ch1.NotifyStateChange(chanStateChanged1)

	chanStateChanged3 := make(chan *StateChanged, 10)
	ch3.NotifyStateChange(chanStateChanged3)

	// Drop connection
	dropConnection(t, connectionName)

	// Wait for connection and channel recovery
	waitForConnectionOpen(t, stateChanged)
	waitForChannelOpen(t, chanStateChanged1)
	waitForChannelOpen(t, chanStateChanged3)

	// Verify channel IDs are reserved in the connection allocator
	conn.m.Lock()
	if conn.allocator == nil {
		conn.m.Unlock()
		t.Fatalf("Expected allocator to be initialized post-recovery")
	}
	isReserved1 := conn.allocator.reserved(int(id1))
	isReserved3 := conn.allocator.reserved(int(id3))
	isReserved2 := conn.allocator.reserved(int(id2))
	isReserved4 := conn.allocator.reserved(int(id4))
	conn.m.Unlock()

	if !isReserved1 {
		t.Fatalf("Expected recovered channel ID %d to be reserved in the allocator", id1)
	}
	if !isReserved3 {
		t.Fatalf("Expected recovered channel ID %d to be reserved in the allocator", id3)
	}
	if isReserved2 {
		t.Fatalf("Expected closed channel ID %d to NOT be reserved in the allocator", id2)
	}
	if isReserved4 {
		t.Fatalf("Expected closed channel ID %d to NOT be reserved in the allocator", id4)
	}
	t.Logf("Verified channel IDs %d and %d are correctly reserved, and %d and %d are free in the allocator", id1, id3, id2, id4)

	// Churn open channels post-recovery and verify they don't conflict with the recovered channel IDs
	var activeChannels []*Channel
	for i := 0; i < 5; i++ {
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("Failed to create channel during churn at iteration %d: %v", i, err)
		}
		activeChannels = append(activeChannels, ch)
		t.Logf("Opened new channel with ID: %d", ch.id)

		if ch.id == id1 || ch.id == id3 {
			t.Fatalf("Conflict detected! New channel allocated with recovered channel ID: %d", ch.id)
		}
	}

	// Close the churned channels
	for _, ch := range activeChannels {
		ch.Close()
	}
}

// TestConnectionRecoveryLifeCycleNotifyStateChange tests that state change listener channels
// are cleanly closed when connection or channels are closed.
func TestConnectionRecoveryLifeCycleNotifyStateChange(t *testing.T) {
	connectionName := "test-connection-recovery-lifecycle-notify-state-change"
	// Create a connection with Recovery
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch1Name := "channel 1"
	ch1, err := conn.Channel()
	if err != nil {
		t.Fatalf("%s creation failed: %v", ch1Name, err)
	}
	defer ch1.Close()

	ch2Name := "channel 2"
	ch2, err := conn.Channel()
	if err != nil {
		t.Fatalf("%s creation failed: %v", ch2Name, err)
	}
	defer ch2.Close()

	// Register state change notifications on connection and channels
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)

	chanStateChanged11 := make(chan *StateChanged, 10)
	ch1.NotifyStateChange(chanStateChanged11)

	// Register one more listener for channel 1
	chanStateChanged12 := make(chan *StateChanged, 10)
	ch1.NotifyStateChange(chanStateChanged12)

	chanStateChanged21 := make(chan *StateChanged, 10)
	ch2.NotifyStateChange(chanStateChanged21)

	// Register one more listener for channel 2
	chanStateChanged22 := make(chan *StateChanged, 10)
	ch2.NotifyStateChange(chanStateChanged22)

	// Drop connection
	dropConnection(t, connectionName)

	// Wait for connection and channels to recover
	waitForConnectionOpen(t, stateChanged)
	waitForChannelOpen(t, chanStateChanged11)
	waitForChannelOpen(t, chanStateChanged21)

	// Close channels
	if err := ch1.Close(); err != nil {
		t.Fatalf("Failed to close %s: %v", ch1Name, err)
	}
	if err := ch2.Close(); err != nil {
		t.Fatalf("Failed to close %s: %v", ch2Name, err)
	}

	// Verify channel 1 listener 1 is cleanly closed within a timeout
	waitForStateChangeClose(t, chanStateChanged11, ch1Name, "listener 1")

	// Verify channel 1 listener 2 is cleanly closed within a timeout
	waitForStateChangeClose(t, chanStateChanged12, ch1Name, "listener 2")

	// Verify channel 2 listener is cleanly closed within a timeout
	waitForStateChangeClose(t, chanStateChanged21, ch2Name, "listener 1")

	// Verify channel 2 listener 2 is cleanly closed within a timeout
	waitForStateChangeClose(t, chanStateChanged22, ch2Name, "listener 2")

	// Close connection
	if err := conn.Close(); err != nil {
		t.Fatalf("Failed to close %s: %v", connectionName, err)
	}

	// Verify connection listener is cleanly closed within a timeout
	waitForStateChangeClose(t, stateChanged, connectionName, "listener 1")
}

// TestConnectionRecoveryCancelInterrupt tests that connection and channel recovery cancel events
// are received when the connection or channel is closed during an active recovery process.
func TestConnectionRecoveryCancelInterrupt(t *testing.T) {
	connectionName := "test-connection-recovery-cancel-interrupt"
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery: &Recovery{
			ReconnectionConfig: &ReconnectionConfig{
				MaxRetryCount: 5,
				RetryInterval: 5 * time.Second,
			},
		},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	// 1. Register for NotifyRecoveryCancel on connection and channel
	connCancelCh := conn.NotifyRecoveryCancel(make(chan struct{}))
	chCancelCh := ch.NotifyRecoveryCancel(make(chan struct{}))

	// Register state change notifications on connection and channel
	connStateCh := make(chan *StateChanged, 10)
	conn.NotifyStateChange(connStateCh)

	chanStateCh := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateCh)

	// 2. Drop connection
	dropConnection(t, connectionName)

	// 3. Wait for status to change to Reconnecting for both connection and channel
	var connReconnectingSeen bool
	var chanReconnectingSeen bool

	timeout := time.After(10 * time.Second)
	for !connReconnectingSeen || !chanReconnectingSeen {
		select {
		case sc := <-connStateCh:
			t.Logf("Connection state changed: %s", sc)
			if sc.To == StateReconnecting {
				connReconnectingSeen = true
			}
		case sc := <-chanStateCh:
			t.Logf("Channel state changed: %s", sc)
			if sc.To == StateReconnecting {
				chanReconnectingSeen = true
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for connection and channel to enter Reconnecting state")
		}
	}

	// 4. Close channel
	t.Log("Closing channel during recovery to trigger abort...")
	if err := ch.Close(); err != nil {
		t.Fatalf("Channel Close failed: %v", err)
	}

	// 5. Close connection
	t.Log("Closing connection during recovery to trigger abort...")
	if err := conn.Close(); err != nil {
		t.Fatalf("Connection Close failed: %v", err)
	}

	// 6. Verify immediately recovery is terminated and event is received on NotifyRecoveryCancel channel
	select {
	case <-chCancelCh:
		t.Log("Channel recovery cancel event received successfully")
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for channel recovery cancel event")
	}

	select {
	case <-connCancelCh:
		t.Log("Connection recovery cancel event received successfully")
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for connection recovery cancel event")
	}

	// 7. Verify state changed to Closed
	var connClosedSeen bool
	var chanClosedSeen bool

	timeout = time.After(2 * time.Second)
	for !connClosedSeen || !chanClosedSeen {
		select {
		case sc, ok := <-connStateCh:
			if !ok {
				connClosedSeen = true
				continue
			}
			t.Logf("Connection state changed post-close: %s", sc)
			if sc.To == StateClosed {
				connClosedSeen = true
			}
		case sc, ok := <-chanStateCh:
			if !ok {
				chanClosedSeen = true
				continue
			}
			t.Logf("Channel state changed post-close: %s", sc)
			if sc.To == StateClosed {
				chanClosedSeen = true
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for connection and channel state to change to Closed. connClosedSeen=%t, chanClosedSeen=%t", connClosedSeen, chanClosedSeen)
		}
	}
}

// TestConnectionRecoveryExclusiveQueue tests recovery of a transient exclusive queue with server generated name,
// an auto-delete exchange, and its binding and consumer, confirming server queue name change handles properly.
func TestConnectionRecoveryExclusiveQueue(t *testing.T) {
	connectionName := "test-connection-recovery-exclusive-queue"

	// 1. DialConfig with default recovery configuration
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	// 2. Create a channel
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	// Create exchange non-durable auto-delete
	exchangeName := "test_recovery_exclusive_exch"
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		false,        // durable
		true,         // auto-delete
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}
	defer func() {
		_ = ch.ExchangeDelete(exchangeName, false, false)
	}()

	// Create transient exclusive queue with server generated name
	queue, err := ch.QueueDeclare(
		"",    // name (empty for server generated)
		false, // durable (transient)
		false, // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	preRecoveryQueueName := queue.Name
	if preRecoveryQueueName == "" {
		t.Fatalf("Expected non-empty server generated queue name")
	}

	// Bind queue to exchange
	routingKey := "test-routing-key"
	err = ch.QueueBind(
		preRecoveryQueueName, // queue
		routingKey,           // routing key
		exchangeName,         // exchange
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	// 3. Create a consumer and start consuming the message, create publisher to publish the message
	msgs, err := ch.Consume(
		preRecoveryQueueName,
		"test-recovery-exclusive-consumer", // consumer tag
		false,                              // auto-ack
		false,                              // exclusive
		false,                              // no-local
		false,                              // no-wait
		nil,                                // args
	)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Publish message
	err = ch.PublishWithContext(
		context.Background(),
		exchangeName, // exchange
		routingKey,   // routing key
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello pre-recovery"),
		},
	)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Read message from consumer to verify consuming works pre-recovery
	select {
	case msg, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed prematurely")
		}
		if string(msg.Body) != "hello pre-recovery" {
			t.Fatalf("Expected message 'hello pre-recovery', got: %s", string(msg.Body))
		}
		t.Logf("Received message pre-recovery: %s. Acking.", string(msg.Body))
		err = msg.Ack(false)
		if err != nil {
			t.Fatalf("Ack failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for message pre-recovery")
	}

	// 4. Record the current topology using channel.TopologyConfiguration
	preRecoveryTopology := ch.TopologyConfiguration(true)

	// Verify old queue name was indeed recorded in pre-recovery topology
	if _, found := preRecoveryTopology.Queues[preRecoveryQueueName]; !found {
		t.Fatalf("Expected old queue name %q to be present in pre-recovery topology, but it was not", preRecoveryQueueName)
	}

	// Verify exchange was indeed recorded in pre-recovery topology
	if _, found := preRecoveryTopology.Exchanges[exchangeName]; !found {
		t.Fatalf("Expected exchange %q to be present in pre-recovery topology, but it was not", exchangeName)
	}

	// 5. Register NotifyStateChange listener
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)

	chanStateChanged := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateChanged)

	// 6. Drop the connection
	dropConnection(t, connectionName)

	// 7. Wait for connection and channel open
	waitForConnectionOpen(t, stateChanged)
	waitForChannelOpen(t, chanStateChanged)

	// 8. Verify topology is recovered by comparing with channel.TopologyConfiguration (Note, server queue name changed is confirmed)
	// and actual queue declare passive and exchange declare passive
	postRecoveryTopology := ch.TopologyConfiguration(true)

	// Verify length of queues in topology
	if len(postRecoveryTopology.Queues) != 1 {
		t.Fatalf("Expected 1 queue in post-recovery topology, got %d", len(postRecoveryTopology.Queues))
	}

	// The old queue name should not be a key in the post-recovery topology queues
	if _, found := postRecoveryTopology.Queues[preRecoveryQueueName]; found {
		t.Fatalf("Expected old queue name %q to be removed from post-recovery topology queues, but it was found", preRecoveryQueueName)
	}

	// Get the new queue name from the map keys
	var postRecoveryQueueName string
	for name, qConfig := range postRecoveryTopology.Queues {
		postRecoveryQueueName = name
		if qConfig.DeclaredName != "" {
			t.Fatalf("Expected DeclaredName of recovered queue to be empty, got %q", qConfig.DeclaredName)
		}
		if qConfig.ActualName != postRecoveryQueueName {
			t.Fatalf("Expected ActualName of recovered queue to be %q, got %q", postRecoveryQueueName, qConfig.ActualName)
		}
	}

	if postRecoveryQueueName == "" {
		t.Fatalf("Expected recovered queue name to be non-empty")
	}

	// Confirm server queue name changed
	if postRecoveryQueueName == preRecoveryQueueName {
		t.Fatalf("Expected server generated queue name to change after recovery, but it remained %q", preRecoveryQueueName)
	}
	t.Logf("Confirmed server-generated queue name changed from %q to %q", preRecoveryQueueName, postRecoveryQueueName)

	// Verify bindings updated with new queue name.
	// We verify that we have exactly 1 binding, and it refers to the new queue name.
	if len(postRecoveryTopology.Bindings) != 1 {
		t.Fatalf("Expected exactly 1 binding in post-recovery topology, got %d", len(postRecoveryTopology.Bindings))
	}
	for _, recoveredBinding := range postRecoveryTopology.Bindings {
		if recoveredBinding.Queue != postRecoveryQueueName {
			t.Fatalf("Expected recovered binding to be for new queue name %q, got %q", postRecoveryQueueName, recoveredBinding.Queue)
		}
		if recoveredBinding.Exchange != exchangeName {
			t.Fatalf("Expected recovered binding to use exchange %q, got %q", exchangeName, recoveredBinding.Exchange)
		}
	}

	// Verify using actual queue declare passive
	_, err = ch.QueueDeclarePassive(
		postRecoveryQueueName,
		false, // durable
		false, // autoDelete
		true,  // exclusive
		false, // noWait
		nil,   // args
	)
	if err != nil {
		t.Fatalf("QueueDeclarePassive failed for recovered queue %q: %v", postRecoveryQueueName, err)
	}

	// Verify using actual exchange declare passive
	err = ch.ExchangeDeclarePassive(
		exchangeName,
		"direct",
		false, // durable
		true,  // autoDelete
		false, // internal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		t.Fatalf("ExchangeDeclarePassive failed for recovered exchange %q: %v", exchangeName, err)
	}

	// 9. Verify the consumer continues receive the messages after topology recovery
	// Publish a post-recovery message
	err = ch.PublishWithContext(
		context.Background(),
		exchangeName, // exchange
		routingKey,   // routing key
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello post-recovery"),
		},
	)
	if err != nil {
		t.Fatalf("Publish failed post-recovery: %v", err)
	}

	// Read from consumer channel
	select {
	case msg, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed after recovery")
		}
		if string(msg.Body) != "hello post-recovery" {
			t.Fatalf("Expected message 'hello post-recovery', got: %s", string(msg.Body))
		}
		t.Logf("Received message post-recovery: %s. Acking.", string(msg.Body))
		err = msg.Ack(false)
		if err != nil {
			t.Fatalf("Ack failed post-recovery: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for post-recovery message")
	}
}

// TestConnectionRecoveryDeletedQueueSkip tests that when a queue is deleted,
// its tracked topology and consumers are successfully removed so that subsequent
// recovery attempts do not try to recover the deleted queue or its consumer,
// allowing the remaining queue's consumer to recover and function correctly.
func TestConnectionRecoveryDeletedQueueSkip(t *testing.T) {
	connectionName := "test-connection-recovery-deleted-queue-skip"

	// 1. Create connection with default recovery
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	// 2. Create channel
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	exchangeName := "test_recovery_deleted_q_exch"
	err = ch.ExchangeDeclare(
		exchangeName,
		"direct",
		false, // durable
		true,  // auto-delete
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}
	defer func() {
		_ = ch.ExchangeDelete(exchangeName, false, false)
	}()

	// Declare Queue 1 and Queue 2 as transient (non-durable) and exclusive
	q1Name := "test_recovery_deleted_q1_transient"
	_, err = ch.QueueDeclare(q1Name, false, false, true, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare q1 failed: %v", err)
	}

	q2Name := "test_recovery_deleted_q2_transient"
	_, err = ch.QueueDeclare(q2Name, false, false, true, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare q2 failed: %v", err)
	}

	// Bind both queues
	err = ch.QueueBind(q1Name, "key1", exchangeName, false, nil)
	if err != nil {
		t.Fatalf("QueueBind q1 failed: %v", err)
	}
	err = ch.QueueBind(q2Name, "key2", exchangeName, false, nil)
	if err != nil {
		t.Fatalf("QueueBind q2 failed: %v", err)
	}

	// 4. Consume from both queues
	msgs1, err := ch.Consume(q1Name, "consumer-q1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume q1 failed: %v", err)
	}

	msgs2, err := ch.Consume(q2Name, "consumer-q2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume q2 failed: %v", err)
	}

	// 5. Delete one queue (q1Name)
	_, err = ch.QueueDelete(q1Name, false, false, false)
	if err != nil {
		t.Fatalf("QueueDelete q1 failed: %v", err)
	}

	// Ensure the consumer channel for q1 is closed due to deletion
	select {
	case _, ok := <-msgs1:
		if ok {
			t.Fatalf("Expected msg channel for deleted queue to be closed, but received a message or it remains open")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for deleted queue's consumer channel to close")
	}

	// 6. Register state change channels to wait for recovery
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)

	chanStateChanged := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateChanged)

	// Drop Connection
	dropConnection(t, connectionName)

	// 7. Wait for recovery
	waitForConnectionOpen(t, stateChanged)
	waitForChannelOpen(t, chanStateChanged)

	// 8. Make sure we can keep consuming from remaining queue (q2Name)
	// Publish to remaining queue
	err = ch.PublishWithContext(
		context.Background(),
		exchangeName,
		"key2",
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello post-recovery-q2"),
		},
	)
	if err != nil {
		t.Fatalf("Publish to q2 failed post-recovery: %v", err)
	}

	// Consume and verify from q2
	select {
	case msg, ok := <-msgs2:
		if !ok {
			t.Fatalf("Consume channel for remaining queue (q2) closed after recovery")
		}
		if string(msg.Body) != "hello post-recovery-q2" {
			t.Fatalf("Expected message 'hello post-recovery-q2', got: %s", string(msg.Body))
		}
		t.Logf("Received message from q2 post-recovery: %s. Acking.", string(msg.Body))
		err = msg.Ack(false)
		if err != nil {
			t.Fatalf("Ack failed on q2 post-recovery: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for message on remaining queue (q2) post-recovery")
	}
}

// TestConnectionRecoveryTopologyOnlyTransient verifies that with TopologyRecoveryMode
// set to TopologyRecoveryOnlyTransient, only connection-scoped (transient) entities are
// recovered. An auto-delete exchange, an exclusive queue, their binding and consumer are
// restored and keep working, while a durable queue is NOT re-declared by the client.
//
// To observe that the durable queue is skipped, it is deleted out-of-band via a separate
// non-recovering connection before the drop. The test channel still tracks it, so under
// TopologyRecoveryAllEnabled it would be re-declared; under OnlyTransient it must remain
// absent.
func TestConnectionRecoveryTopologyOnlyTransient(t *testing.T) {
	connectionName := "test-connection-recovery-only-transient"

	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery: &Recovery{
			TopologyRecoveryMode: TopologyRecoveryOnlyTransient,
		},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	// --- Durable topology (must NOT be recovered by the client) ---
	// The durable queue is bound to a durable exchange so that neither the queue nor
	// the binding qualifies as transient.
	durableExchange := "test_only_transient_durable_ex"
	if err := ch.ExchangeDeclare(durableExchange, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("durable ExchangeDeclare failed: %v", err)
	}
	defer func() {
		if !conn.IsClosed() {
			_ = ch.ExchangeDelete(durableExchange, false, false)
		}
	}()

	durableQueue := "test_only_transient_durable_q"
	if _, err := ch.QueueDeclare(durableQueue, true, false, false, false, nil); err != nil {
		t.Fatalf("durable QueueDeclare failed: %v", err)
	}
	if err := ch.QueueBind(durableQueue, "durable-key", durableExchange, false, nil); err != nil {
		t.Fatalf("durable QueueBind failed: %v", err)
	}

	// --- Transient topology (must be recovered) ---
	transientExchange := "test_only_transient_ex"
	if err := ch.ExchangeDeclare(transientExchange, "direct", false, true, false, false, nil); err != nil {
		t.Fatalf("transient ExchangeDeclare failed: %v", err)
	}

	transientQueue := "test_only_transient_q"
	if _, err := ch.QueueDeclare(transientQueue, false, false, true, false, nil); err != nil { // exclusive
		t.Fatalf("transient QueueDeclare failed: %v", err)
	}
	transientKey := "transient-key"
	if err := ch.QueueBind(transientQueue, transientKey, transientExchange, false, nil); err != nil {
		t.Fatalf("transient QueueBind failed: %v", err)
	}

	msgs, err := ch.Consume(transientQueue, "only-transient-consumer", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Sanity: routing through the transient topology works pre-recovery.
	if err := ch.PublishWithContext(context.Background(), transientExchange, transientKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("pre-recovery")}); err != nil {
		t.Fatalf("pre-recovery publish failed: %v", err)
	}
	select {
	case d, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed prematurely")
		}
		if string(d.Body) != "pre-recovery" {
			t.Fatalf("Expected 'pre-recovery', got %q", string(d.Body))
		}
		t.Logf("Received message pre-recovery: %s", string(d.Body))
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for pre-recovery message")
	}

	// Delete the durable queue out-of-band on a separate non-recovering connection.
	// The test channel still tracks it, so its recovery behavior is observable.
	adminConn, err := DialConfig(amqpURL, Config{Locale: defaultLocale})
	if err != nil {
		t.Fatalf("admin DialConfig failed: %v", err)
	}
	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("admin Channel failed: %v", err)
	}
	if _, err := adminCh.QueueDelete(durableQueue, false, false, false); err != nil {
		t.Fatalf("admin QueueDelete failed: %v", err)
	}
	_ = adminCh.Close()
	_ = adminConn.Close()

	// Register state listeners and drop the connection.
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)
	chanStateChanged := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateChanged)

	dropConnection(t, connectionName)

	waitForConnectionOpen(t, stateChanged)
	waitForChannelOpen(t, chanStateChanged)

	// --- Assertion 1: the transient queue, binding and consumer were recovered ---
	if err := ch.PublishWithContext(context.Background(), transientExchange, transientKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("post-recovery")}); err != nil {
		t.Fatalf("post-recovery publish failed: %v", err)
	}
	select {
	case d, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed after recovery")
		}
		if string(d.Body) != "post-recovery" {
			t.Fatalf("Expected 'post-recovery', got %q", string(d.Body))
		}
		t.Logf("Transient queue recovered; received post-recovery message: %s", string(d.Body))
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for post-recovery message on recovered transient queue")
	}

	// --- Assertion 2: the durable queue was NOT re-declared during recovery ---
	// A failed passive declare closes the channel, so use a throwaway channel.
	checkCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("verification Channel failed: %v", err)
	}
	defer checkCh.Close()
	_, err = checkCh.QueueDeclarePassive(durableQueue, true, false, false, false, nil)
	if err == nil {
		t.Fatalf("Expected durable queue %q to be absent after OnlyTransient recovery, but it exists", durableQueue)
	}
	amqpErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("Expected *Error from passive declare, got %T: %v", err, err)
	}
	if amqpErr.Code != NotFound {
		t.Fatalf("Expected NotFound (404) for skipped durable queue, got code %d", amqpErr.Code)
	}
	t.Logf("Confirmed durable queue %q was not re-declared during OnlyTransient recovery", durableQueue)
}

// TestConnectionRecoveryMultiChannelTopology tests recovery when topology is split across two channels:
// channel 1 declares a transient exchange and server-named exclusive queue, while channel 2
// creates the binding and consumer. After connection recovery both channels and the full
// topology (exchange → binding → queue → consumer) must be functional.
func TestConnectionRecoveryMultiChannelTopology(t *testing.T) {
	connectionName := "test-connection-recovery-multi-channel-topology"

	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	// --- Step 1: Channel 1 declares transient exchange and server-named exclusive queue ---
	ch1, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 1 creation failed: %v", err)
	}
	defer ch1.Close()

	exchangeName := "test_multi_chan_topology_ex"
	if err := ch1.ExchangeDeclare(
		exchangeName,
		"direct",
		false, // durable
		true,  // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		t.Fatalf("ExchangeDeclare on ch1 failed: %v", err)
	}
	defer func() {
		_ = ch1.ExchangeDelete(exchangeName, false, false)
	}()

	queue, err := ch1.QueueDeclare(
		"",    // server-generated name
		false, // durable
		false, // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		t.Fatalf("QueueDeclare on ch1 failed: %v", err)
	}
	preRecoveryQueueName := queue.Name
	t.Logf("Server-generated queue name pre-recovery: %q", preRecoveryQueueName)

	// --- Step 2: Channel 2 declares the binding ---
	ch2, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 2 creation failed: %v", err)
	}
	defer ch2.Close()

	routingKey := "multi-chan-key"
	if err := ch2.QueueBind(
		preRecoveryQueueName,
		routingKey,
		exchangeName,
		false,
		nil,
	); err != nil {
		t.Fatalf("QueueBind on ch2 failed: %v", err)
	}

	// --- Step 3: Start consumer on channel 2 ---
	msgs, err := ch2.Consume(
		preRecoveryQueueName,
		"multi-chan-consumer",
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		t.Fatalf("Consume on ch2 failed: %v", err)
	}

	// --- Step 4: Publish message using channel 1 ---
	if err := ch1.PublishWithContext(
		context.Background(),
		exchangeName,
		routingKey,
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte("pre-recovery message"),
		},
	); err != nil {
		t.Fatalf("Publish on ch1 pre-recovery failed: %v", err)
	}

	// --- Step 5: Confirm the message is received ---
	select {
	case msg, ok := <-msgs:
		if !ok {
			t.Fatalf("Consumer channel closed prematurely")
		}
		if string(msg.Body) != "pre-recovery message" {
			t.Fatalf("Expected 'pre-recovery message', got %q", string(msg.Body))
		}
		t.Logf("Received pre-recovery message: %s", string(msg.Body))
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for pre-recovery message")
	}

	// --- Step 6: Register state change listeners ---
	connStateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(connStateChanged)

	ch1StateChanged := make(chan *StateChanged, 10)
	ch1.NotifyStateChange(ch1StateChanged)

	ch2StateChanged := make(chan *StateChanged, 10)
	ch2.NotifyStateChange(ch2StateChanged)

	// Drop the connection
	dropConnection(t, connectionName)

	// --- Step 7: Wait for connection and both channels to recover ---
	waitForConnectionOpen(t, connStateChanged)
	waitForChannelOpen(t, ch1StateChanged)
	waitForChannelOpen(t, ch2StateChanged)

	// Confirm the server-generated queue name was updated after recovery (it will differ).
	postRecoveryTopology := ch1.TopologyConfiguration(true)
	if len(postRecoveryTopology.Queues) != 1 {
		t.Fatalf("Expected 1 queue in ch2 post-recovery topology, got %d", len(postRecoveryTopology.Queues))
	}
	var postRecoveryQueueName string
	for name := range postRecoveryTopology.Queues {
		postRecoveryQueueName = name
	}
	if postRecoveryQueueName == preRecoveryQueueName {
		t.Logf("Note: server-generated queue name did not change (%q); this can happen when the broker reuses the name", postRecoveryQueueName)
	} else {
		t.Logf("Server-generated queue name changed from %q to %q after recovery", preRecoveryQueueName, postRecoveryQueueName)
	}

	// --- Step 8: Confirm messages continue to be received after recovery ---
	if err := ch1.PublishWithContext(
		context.Background(),
		exchangeName,
		routingKey,
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte("post-recovery message"),
		},
	); err != nil {
		t.Fatalf("Publish on ch1 post-recovery failed: %v", err)
	}

	select {
	case msg, ok := <-msgs:
		if !ok {
			t.Fatalf("Consumer channel closed after recovery")
		}
		if string(msg.Body) != "post-recovery message" {
			t.Fatalf("Expected 'post-recovery message', got %q", string(msg.Body))
		}
		t.Logf("Received post-recovery message: %s", string(msg.Body))
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for post-recovery message")
	}
}

// TestConnectionRecoveryTopologyAllEnabled tests that TopologyRecoveryAllEnabled recovers all
// tracked topology across channels, including durable entities and transient entities.
//
// Topology layout:
//   - ch1 declares a durable non-auto-delete exchange and a durable non-exclusive queue.
//     ch2 creates the binding and consumer for those durable entities.
//   - ch2 declares a transient (auto-delete) exchange and an exclusive queue.
//     ch1 creates the binding and consumer for those transient entities.
//
// After the connection is dropped, the broker deletes the transient entities. AllEnabled
// recovery re-declares all entities and re-subscribes all consumers, so both delivery
// channels continue to work post-recovery.
func TestConnectionRecoveryTopologyAllEnabled(t *testing.T) {
	connectionName := "test-connection-recovery-topology-all-enabled"

	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery: &Recovery{
			TopologyRecoveryMode: TopologyRecoveryAllEnabled,
		},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch1, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 1 creation failed: %v", err)
	}
	defer ch1.Close()

	ch2, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 2 creation failed: %v", err)
	}
	defer ch2.Close()

	// ch1: durable exchange + durable queue (non-auto-delete, non-exclusive)
	durableExchange := "test_all_enabled_durable_ex"
	if err := ch1.ExchangeDeclare(durableExchange, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("durable ExchangeDeclare on ch1 failed: %v", err)
	}
	defer func() {
		if !conn.IsClosed() {
			_ = ch1.ExchangeDelete(durableExchange, false, false)
		}
	}()

	durableQueue := "test_all_enabled_durable_q"
	if _, err := ch1.QueueDeclare(durableQueue, true, false, false, false, nil); err != nil {
		t.Fatalf("durable QueueDeclare on ch1 failed: %v", err)
	}
	defer func() {
		if !conn.IsClosed() {
			_, _ = ch1.QueueDelete(durableQueue, false, false, false)
		}
	}()

	// ch2 creates binding and consumer for the durable entities
	durableKey := "all-enabled-durable-key"
	if err := ch2.QueueBind(durableQueue, durableKey, durableExchange, false, nil); err != nil {
		t.Fatalf("durable QueueBind on ch2 failed: %v", err)
	}
	msgsFromDurable, err := ch2.Consume(durableQueue, "consumer-all-enabled-durable", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume from durable queue on ch2 failed: %v", err)
	}

	// ch2: transient (auto-delete) exchange + exclusive queue
	transientExchange := "test_all_enabled_transient_ex"
	if err := ch2.ExchangeDeclare(transientExchange, "direct", false, true, false, false, nil); err != nil {
		t.Fatalf("transient ExchangeDeclare on ch2 failed: %v", err)
	}

	transientQueue := "test_all_enabled_transient_q"
	if _, err := ch2.QueueDeclare(transientQueue, false, false, true, false, nil); err != nil {
		t.Fatalf("transient QueueDeclare on ch2 failed: %v", err)
	}

	// ch1 creates binding and consumer for the transient entities
	transientKey := "all-enabled-transient-key"
	if err := ch1.QueueBind(transientQueue, transientKey, transientExchange, false, nil); err != nil {
		t.Fatalf("transient QueueBind on ch1 failed: %v", err)
	}
	msgsFromTransient, err := ch1.Consume(transientQueue, "consumer-all-enabled-transient", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume from transient queue on ch1 failed: %v", err)
	}

	// Pre-recovery: publish to both exchanges and verify delivery
	if err := ch1.PublishWithContext(context.Background(), durableExchange, durableKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("pre-recovery-durable")}); err != nil {
		t.Fatalf("pre-recovery publish to durable exchange failed: %v", err)
	}
	if err := ch2.PublishWithContext(context.Background(), transientExchange, transientKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("pre-recovery-transient")}); err != nil {
		t.Fatalf("pre-recovery publish to transient exchange failed: %v", err)
	}

	select {
	case msg, ok := <-msgsFromDurable:
		if !ok {
			t.Fatalf("durable consumer channel closed prematurely")
		}
		if string(msg.Body) != "pre-recovery-durable" {
			t.Fatalf("Expected 'pre-recovery-durable', got %q", string(msg.Body))
		}
		t.Logf("Received pre-recovery message from durable queue: %s", string(msg.Body))
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for pre-recovery message from durable queue")
	}

	select {
	case msg, ok := <-msgsFromTransient:
		if !ok {
			t.Fatalf("transient consumer channel closed prematurely")
		}
		if string(msg.Body) != "pre-recovery-transient" {
			t.Fatalf("Expected 'pre-recovery-transient', got %q", string(msg.Body))
		}
		t.Logf("Received pre-recovery message from transient queue: %s", string(msg.Body))
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for pre-recovery message from transient queue")
	}

	// Register state change listeners
	connStateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(connStateChanged)

	ch1StateChanged := make(chan *StateChanged, 10)
	ch1.NotifyStateChange(ch1StateChanged)

	ch2StateChanged := make(chan *StateChanged, 10)
	ch2.NotifyStateChange(ch2StateChanged)

	dropConnection(t, connectionName)

	waitForConnectionOpen(t, connStateChanged)
	waitForChannelOpen(t, ch1StateChanged)
	waitForChannelOpen(t, ch2StateChanged)

	// Post-recovery: publish and verify both consumers still receive messages.
	// The durable entities survived the broker drop; AllEnabled re-subscribes the consumer.
	// The transient entities were deleted by the broker on drop; AllEnabled re-declares them.
	if err := ch1.PublishWithContext(context.Background(), durableExchange, durableKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("post-recovery-durable")}); err != nil {
		t.Fatalf("post-recovery publish to durable exchange failed: %v", err)
	}
	if err := ch2.PublishWithContext(context.Background(), transientExchange, transientKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("post-recovery-transient")}); err != nil {
		t.Fatalf("post-recovery publish to transient exchange failed: %v", err)
	}

	select {
	case msg, ok := <-msgsFromDurable:
		if !ok {
			t.Fatalf("durable consumer channel closed after recovery")
		}
		if string(msg.Body) != "post-recovery-durable" {
			t.Fatalf("Expected 'post-recovery-durable', got %q", string(msg.Body))
		}
		t.Logf("Received post-recovery message from durable queue: %s", string(msg.Body))
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for post-recovery message from durable queue")
	}

	select {
	case msg, ok := <-msgsFromTransient:
		if !ok {
			t.Fatalf("transient consumer channel closed after recovery")
		}
		if string(msg.Body) != "post-recovery-transient" {
			t.Fatalf("Expected 'post-recovery-transient', got %q", string(msg.Body))
		}
		t.Logf("Received post-recovery message from transient queue: %s", string(msg.Body))
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for post-recovery message from transient queue")
	}
}

// TestConnectionRecoveryTopologyDisabled tests that TopologyRecoveryDisabled skips all
// topology and consumer recovery after connection and channel reconnect.
//
// Topology layout:
//   - ch1 declares a durable non-auto-delete exchange and a durable non-exclusive queue.
//     ch2 creates the binding and consumer for those durable entities.
//   - ch2 declares a transient (auto-delete) exchange and an exclusive queue.
//     ch1 creates the binding and consumer for those transient entities.
//
// After the connection is dropped and recovers:
//   - Transient entities are gone (broker deleted them when the connection dropped).
//   - Durable entities are still present (broker retained them).
//   - Neither entity type is re-declared by the client, and consumers are not re-subscribed.
//
// Verification uses a fresh non-recovering connection to confirm the durable exchange
// and queue remain functional after recovery.
func TestConnectionRecoveryTopologyDisabled(t *testing.T) {
	connectionName := "test-connection-recovery-topology-disabled"

	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery: &Recovery{
			TopologyRecoveryMode: TopologyRecoveryDisabled,
		},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch1, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 1 creation failed: %v", err)
	}
	defer ch1.Close()

	ch2, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel 2 creation failed: %v", err)
	}
	defer ch2.Close()

	// ch1: durable exchange + durable queue (non-auto-delete, non-exclusive)
	durableExchange := "test_disabled_durable_ex"
	if err := ch1.ExchangeDeclare(durableExchange, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("durable ExchangeDeclare on ch1 failed: %v", err)
	}
	defer func() {
		if !conn.IsClosed() {
			_ = ch1.ExchangeDelete(durableExchange, false, false)
		}
	}()

	durableQueue := "test_disabled_durable_q"
	if _, err := ch1.QueueDeclare(durableQueue, true, false, false, false, nil); err != nil {
		t.Fatalf("durable QueueDeclare on ch1 failed: %v", err)
	}
	defer func() {
		if !conn.IsClosed() {
			_, _ = ch1.QueueDelete(durableQueue, false, false, false)
		}
	}()

	// ch2 creates binding and consumer for the durable entities
	durableKey := "disabled-durable-key"
	if err := ch2.QueueBind(durableQueue, durableKey, durableExchange, false, nil); err != nil {
		t.Fatalf("durable QueueBind on ch2 failed: %v", err)
	}
	msgsFromDurable, err := ch2.Consume(durableQueue, "consumer-disabled-durable", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume from durable queue on ch2 failed: %v", err)
	}

	// ch2: transient (auto-delete) exchange + exclusive queue
	transientExchange := "test_disabled_transient_ex"
	if err := ch2.ExchangeDeclare(transientExchange, "direct", false, true, false, false, nil); err != nil {
		t.Fatalf("transient ExchangeDeclare on ch2 failed: %v", err)
	}

	transientQueue := "test_disabled_transient_q"
	if _, err := ch2.QueueDeclare(transientQueue, false, false, true, false, nil); err != nil {
		t.Fatalf("transient QueueDeclare on ch2 failed: %v", err)
	}

	// ch1 creates binding and consumer for the transient entities
	transientKey := "disabled-transient-key"
	if err := ch1.QueueBind(transientQueue, transientKey, transientExchange, false, nil); err != nil {
		t.Fatalf("transient QueueBind on ch1 failed: %v", err)
	}
	msgsFromTransient, err := ch1.Consume(transientQueue, "consumer-disabled-transient", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume from transient queue on ch1 failed: %v", err)
	}

	// Pre-recovery: publish to both exchanges and verify delivery
	if err := ch1.PublishWithContext(context.Background(), durableExchange, durableKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("pre-recovery-durable")}); err != nil {
		t.Fatalf("pre-recovery publish to durable exchange failed: %v", err)
	}
	if err := ch2.PublishWithContext(context.Background(), transientExchange, transientKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("pre-recovery-transient")}); err != nil {
		t.Fatalf("pre-recovery publish to transient exchange failed: %v", err)
	}

	select {
	case msg, ok := <-msgsFromDurable:
		if !ok {
			t.Fatalf("durable consumer channel closed prematurely")
		}
		if string(msg.Body) != "pre-recovery-durable" {
			t.Fatalf("Expected 'pre-recovery-durable', got %q", string(msg.Body))
		}
		t.Logf("Received pre-recovery message from durable queue: %s", string(msg.Body))
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for pre-recovery message from durable queue")
	}

	select {
	case msg, ok := <-msgsFromTransient:
		if !ok {
			t.Fatalf("transient consumer channel closed prematurely")
		}
		if string(msg.Body) != "pre-recovery-transient" {
			t.Fatalf("Expected 'pre-recovery-transient', got %q", string(msg.Body))
		}
		t.Logf("Received pre-recovery message from transient queue: %s", string(msg.Body))
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for pre-recovery message from transient queue")
	}

	// Register state change listeners
	connStateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(connStateChanged)

	ch1StateChanged := make(chan *StateChanged, 10)
	ch1.NotifyStateChange(ch1StateChanged)

	ch2StateChanged := make(chan *StateChanged, 10)
	ch2.NotifyStateChange(ch2StateChanged)

	dropConnection(t, connectionName)

	// Connection and channels recover (reconnect) even though topology recovery is disabled.
	waitForConnectionOpen(t, connStateChanged)
	waitForChannelOpen(t, ch1StateChanged)
	waitForChannelOpen(t, ch2StateChanged)

	// Verify transient entities are gone (broker deleted them on connection drop).
	// A failed passive declare closes the channel, so use a throwaway channel for each check.
	checkTransientExCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("verification Channel for transient exchange check failed: %v", err)
	}
	defer checkTransientExCh.Close()
	err = checkTransientExCh.ExchangeDeclarePassive(transientExchange, "direct", false, true, false, false, nil)
	if err == nil {
		t.Fatalf("Expected transient exchange %q to be absent after TopologyRecoveryDisabled, but it exists", transientExchange)
	}
	amqpErr, ok := err.(*Error)
	if !ok || amqpErr.Code != NotFound {
		t.Fatalf("Expected NotFound (404) for absent transient exchange, got: %v", err)
	}
	t.Logf("Confirmed transient exchange %q is absent after TopologyRecoveryDisabled", transientExchange)

	checkTransientQCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("verification Channel for transient queue check failed: %v", err)
	}
	defer checkTransientQCh.Close()
	_, err = checkTransientQCh.QueueDeclarePassive(transientQueue, false, false, true, false, nil)
	if err == nil {
		t.Fatalf("Expected transient queue %q to be absent after TopologyRecoveryDisabled, but it exists", transientQueue)
	}
	amqpErr, ok = err.(*Error)
	if !ok || amqpErr.Code != NotFound {
		t.Fatalf("Expected NotFound (404) for absent transient queue, got: %v", err)
	}
	t.Logf("Confirmed transient queue %q is absent after TopologyRecoveryDisabled", transientQueue)

	// Verify durable entities are still present (broker retained them).
	checkDurableCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("verification Channel for durable entities check failed: %v", err)
	}
	defer checkDurableCh.Close()

	err = checkDurableCh.ExchangeDeclarePassive(durableExchange, "direct", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Expected durable exchange %q to be present after TopologyRecoveryDisabled, but got: %v", durableExchange, err)
	}
	t.Logf("Confirmed durable exchange %q is still present after TopologyRecoveryDisabled", durableExchange)

	_, err = checkDurableCh.QueueDeclarePassive(durableQueue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Expected durable queue %q to be present after TopologyRecoveryDisabled, but got: %v", durableQueue, err)
	}
	t.Logf("Confirmed durable queue %q is still present after TopologyRecoveryDisabled", durableQueue)

	// Verify durable topology is functional via a fresh non-recovering connection.
	freshConn, err := DialConfig(amqpURL, Config{Locale: defaultLocale})
	if err != nil {
		t.Fatalf("fresh DialConfig failed: %v", err)
	}
	defer freshConn.Close()

	freshCh, err := freshConn.Channel()
	if err != nil {
		t.Fatalf("fresh Channel failed: %v", err)
	}
	defer freshCh.Close()

	freshMsgs, err := freshCh.Consume(durableQueue, "consumer-disabled-fresh", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("fresh Consume on durable queue failed: %v", err)
	}

	if err := freshCh.PublishWithContext(context.Background(), durableExchange, durableKey, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("post-recovery-durable")}); err != nil {
		t.Fatalf("fresh publish to durable exchange failed: %v", err)
	}

	select {
	case msg, ok := <-freshMsgs:
		if !ok {
			t.Fatalf("fresh consumer channel closed prematurely")
		}
		if string(msg.Body) != "post-recovery-durable" {
			t.Fatalf("Expected 'post-recovery-durable', got %q", string(msg.Body))
		}
		t.Logf("Received post-recovery message from durable queue via fresh connection: %s", string(msg.Body))
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for post-recovery message via fresh connection")
	}
}

// TestConnectionRecoveryExhaustionDoesNotPanic verifies that when topology
// recovery fails on every retry and recovery is ultimately exhausted, the final
// cleanup() does not double-close the NotifyClose/NotifyBlocked listener channels.
//
// The failure path is: a recoverable drop starts recovery; a queue that can no
// longer be redeclared (its definition was changed out-of-band) makes
// RecoverTopology fail (OnTopologyEntityError returns false to abort, not skip);
// Reconnect() calls Close() on the transport, whose reader raises a
// non-recoverable shutdown that closes the listeners; after retries are exhausted
// OnConnectionClose calls cleanup(), which closed the same listeners a second time
// and panicked with "close of closed channel". A registered NotifyClose listener
// is required to surface the panic.
func TestConnectionRecoveryExhaustionDoesNotPanic(t *testing.T) {
	connectionName := "test-connection-recovery-exhaustion-no-panic"
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)

	conn, err := DialConfig(amqpURL, Config{
		Recovery: &Recovery{
			ReconnectionConfig: &ReconnectionConfig{
				MaxRetryCount: 2,
				RetryInterval: 1 * time.Second,
			},
			TopologyRecoveryMode: TopologyRecoveryAllEnabled,
			// Return false to abort topology recovery on any entity error.
			// Without this, the default behavior skips the failed entity and
			// recovery succeeds — the connection never reaches StateClosed.
			OnTopologyEntityError: func(_ *Connection, _ TopologyRecoveryEntity) bool {
				return false
			},
		},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}

	// A durable queue tracked for recovery. We will change its definition
	// out-of-band so the recovery-time redeclare fails with precondition_failed.
	queueName := "test_recovery_exhaustion_q"
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, Table{"x-max-length": int32(10)}); err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}
	defer func() {
		// Clean up via a fresh connection; the recovering one is expected to die.
		cleanupConn, derr := DialConfig(amqpURL, Config{Locale: defaultLocale})
		if derr != nil {
			return
		}
		defer cleanupConn.Close()
		if cleanupCh, cerr := cleanupConn.Channel(); cerr == nil {
			_, _ = cleanupCh.QueueDelete(queueName, false, false, false)
		}
	}()

	// Register a NotifyClose listener: this is the channel that cleanup()
	// double-closed. Drain it concurrently (the realistic usage) so the listener
	// never blocks shutdown's send; a blocked send would exercise an unrelated,
	// pre-existing send/close race in shutdown() rather than the double-close this
	// test targets. The drain goroutine exits when the channel is finally closed.
	closeCh := conn.NotifyClose(make(chan *Error, 1))
	closeDrained := make(chan struct{})
	go func() {
		defer close(closeDrained)
		for range closeCh {
		}
	}()

	// Out-of-band: delete and redeclare the queue with a conflicting definition so
	// the client's recovery redeclare fails.
	adminConn, err := DialConfig(amqpURL, Config{Locale: defaultLocale})
	if err != nil {
		t.Fatalf("admin DialConfig failed: %v", err)
	}
	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("admin Channel failed: %v", err)
	}
	if _, err := adminCh.QueueDelete(queueName, false, false, false); err != nil {
		t.Fatalf("admin QueueDelete failed: %v", err)
	}
	if _, err := adminCh.QueueDeclare(queueName, true, false, false, false, Table{"x-max-length": int32(99)}); err != nil {
		t.Fatalf("admin QueueDeclare failed: %v", err)
	}
	_ = adminConn.Close()

	stateChanged := make(chan *StateChanged, 20)
	conn.NotifyStateChange(stateChanged)

	// Drop the client connection to trigger recovery, which will fail to recover
	// the now-conflicting queue and eventually exhaust retries.
	dropConnection(t, connectionName)

	// Wait for the connection to reach its terminal Closed state. If cleanup()
	// double-closes the listeners it panics and crashes the test binary, so simply
	// reaching this state without a panic is the assertion.
	timeout := time.After(30 * time.Second)
	for {
		select {
		case sc := <-stateChanged:
			t.Logf("Connection state changed: %s", sc)
			if sc.To == StateClosed {
				goto closed
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for connection to reach StateClosed after recovery exhaustion")
		}
	}

closed:
	// The NotifyClose listener must be closed (not double-closed -> panic) exactly
	// once. The drain goroutine returns only when the channel is closed.
	select {
	case <-closeDrained:
		// Listener was finalized and closed cleanly.
	case <-time.After(5 * time.Second):
		t.Fatalf("NotifyClose listener was not closed after recovery exhaustion")
	}

	if !conn.IsClosed() {
		t.Fatalf("expected connection to be closed after recovery exhaustion")
	}
	t.Log("Recovery exhaustion completed without panicking on listener cleanup")
}

// TestConnectionRecoveryAutoDeleteTopologyForgotten verifies "forget on auto-delete" behaviour:
//
//  1. An auto-delete queue is forgotten from tracking only when its LAST consumer
//     is cancelled.  Cancelling the first of two consumers must keep the queue tracked.
//
//  2. An auto-delete exchange is forgotten from tracking when its last queue-binding
//     is removed via QueueUnbind.
//
//  3. After connection recovery, neither the forgotten queue nor the forgotten exchange
//     are re-declared on the broker — a passive declare returns NotFound.
func TestConnectionRecoveryAutoDeleteTopologyForgotten(t *testing.T) {
	connectionName := "test-connection-recovery-auto-delete-forgotten"

	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	// -----------------------------------------------------------------------
	// Part 1 — auto-delete queue: forgotten only after the LAST consumer cancel
	// -----------------------------------------------------------------------

	adQueueExchange := "test_auto_delete_queue_exchange"
	err = ch.ExchangeDeclare(adQueueExchange, "direct", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}
	defer func() { _ = ch.ExchangeDelete(adQueueExchange, false, false) }()

	adQueue := "test_auto_delete_queue"
	_, err = ch.QueueDeclare(adQueue, false, true, true, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	err = ch.QueueBind(adQueue, "ad-key", adQueueExchange, false, nil)
	if err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	// Register two consumers.
	_, err = ch.Consume(adQueue, "consumer-ad-1", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume (consumer 1) failed: %v", err)
	}
	_, err = ch.Consume(adQueue, "consumer-ad-2", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume (consumer 2) failed: %v", err)
	}

	// Cancel the first consumer — queue must still be tracked.
	if err := ch.Cancel("consumer-ad-1", false); err != nil {
		t.Fatalf("Cancel consumer-ad-1 failed: %v", err)
	}
	topology := ch.TopologyConfiguration(true)
	if _, found := topology.Queues[adQueue]; !found {
		t.Fatalf("Expected auto-delete queue %q to remain tracked after first consumer cancel (second still active)", adQueue)
	}
	t.Logf("After first cancel: queue %q is correctly still tracked", adQueue)

	// Cancel the second (last) consumer — queue AND its binding must be forgotten.
	if err := ch.Cancel("consumer-ad-2", false); err != nil {
		t.Fatalf("Cancel consumer-ad-2 failed: %v", err)
	}
	topology = ch.TopologyConfiguration(true)
	if _, found := topology.Queues[adQueue]; found {
		t.Fatalf("Expected auto-delete queue %q to be forgotten after last consumer cancel, but it is still tracked", adQueue)
	}
	t.Logf("After last cancel: queue %q is correctly forgotten", adQueue)

	// The exchange sourced a binding to the (now-gone) queue; it must also be forgotten.
	if _, found := topology.Exchanges[adQueueExchange]; found {
		t.Fatalf("Expected auto-delete exchange %q to be forgotten after its last binding was removed, but it is still tracked", adQueueExchange)
	}
	t.Logf("Auto-delete exchange %q was correctly cascade-forgotten", adQueueExchange)

	// -----------------------------------------------------------------------
	// Part 2 — auto-delete exchange: forgotten via QueueUnbind
	// -----------------------------------------------------------------------

	adExchange := "test_auto_delete_exchange_unbind"
	err = ch.ExchangeDeclare(adExchange, "direct", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare (ad exchange) failed: %v", err)
	}
	defer func() { _ = ch.ExchangeDelete(adExchange, false, false) }()

	// Use a durable, non-auto-delete queue so only the exchange is the auto-delete entity.
	durableQueue := "test_auto_delete_exchange_durable_q"
	_, err = ch.QueueDeclare(durableQueue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare (durable) failed: %v", err)
	}
	defer func() { _, _ = ch.QueueDelete(durableQueue, false, false, false) }()

	err = ch.QueueBind(durableQueue, "unbind-key", adExchange, false, nil)
	if err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	// Confirm the exchange is tracked pre-unbind.
	topology = ch.TopologyConfiguration(true)
	if _, found := topology.Exchanges[adExchange]; !found {
		t.Fatalf("Expected auto-delete exchange %q to be tracked before unbind", adExchange)
	}

	// Remove the binding — exchange should be forgotten immediately.
	err = ch.QueueUnbind(durableQueue, "unbind-key", adExchange, nil)
	if err != nil {
		t.Fatalf("QueueUnbind failed: %v", err)
	}
	topology = ch.TopologyConfiguration(true)
	if _, found := topology.Exchanges[adExchange]; found {
		t.Fatalf("Expected auto-delete exchange %q to be forgotten after last QueueUnbind, but it is still tracked", adExchange)
	}
	t.Logf("Auto-delete exchange %q correctly forgotten after QueueUnbind", adExchange)

	// -----------------------------------------------------------------------
	// Part 3 — recovery must NOT re-declare forgotten entities on the broker
	// -----------------------------------------------------------------------

	// Declare a surviving entity so we have something to confirm recovery ran.
	survivorQueue := "test_auto_delete_forgotten_survivor"
	_, err = ch.QueueDeclare(survivorQueue, false, false, true, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare (survivor) failed: %v", err)
	}
	survivorMsgs, err := ch.Consume(survivorQueue, "consumer-survivor", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume (survivor) failed: %v", err)
	}

	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)
	chanStateChanged := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateChanged)

	dropConnection(t, connectionName)

	waitForConnectionOpen(t, stateChanged)
	waitForChannelOpen(t, chanStateChanged)

	// Confirm recovery ran: publish to the survivor queue and wait for delivery.
	if err := ch.PublishWithContext(
		context.Background(),
		"", survivorQueue, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("recovery-probe")},
	); err != nil {
		t.Fatalf("publish to survivor queue post-recovery failed: %v", err)
	}
	select {
	case msg, ok := <-survivorMsgs:
		if !ok {
			t.Fatalf("survivor consumer channel closed after recovery")
		}
		if string(msg.Body) != "recovery-probe" {
			t.Fatalf("Expected 'recovery-probe', got %q", string(msg.Body))
		}
		t.Logf("Survivor consumer received post-recovery message: %s", string(msg.Body))
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for post-recovery message on survivor queue")
	}

	// The auto-delete queue and exchange were forgotten before the drop, so recovery
	// must not re-declare them.  Use fresh throwaway channels for passive declares
	// (a failed passive declare closes its channel).
	checkQCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("verification channel for queue check failed: %v", err)
	}
	defer checkQCh.Close()
	_, err = checkQCh.QueueDeclarePassive(adQueue, false, true, true, false, nil)
	if err == nil {
		t.Fatalf("Expected auto-delete queue %q to be absent after recovery (was forgotten), but it exists", adQueue)
	}
	amqpErr, ok := err.(*Error)
	if !ok || amqpErr.Code != NotFound {
		t.Fatalf("Expected NotFound (404) for absent auto-delete queue, got: %v", err)
	}
	t.Logf("Confirmed auto-delete queue %q was NOT re-declared during recovery", adQueue)

	checkExCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("verification channel for exchange check failed: %v", err)
	}
	defer checkExCh.Close()
	err = checkExCh.ExchangeDeclarePassive(adQueueExchange, "direct", false, true, false, false, nil)
	if err == nil {
		t.Fatalf("Expected auto-delete exchange %q to be absent after recovery (was forgotten), but it exists", adQueueExchange)
	}
	amqpErr, ok = err.(*Error)
	if !ok || amqpErr.Code != NotFound {
		t.Fatalf("Expected NotFound (404) for absent auto-delete exchange, got: %v", err)
	}
	t.Logf("Confirmed auto-delete exchange %q was NOT re-declared during recovery", adQueueExchange)
}

// TestConnectionRecoveryAutoDeleteExchangeCascade verifies four cascade scenarios
// for auto-delete exchange forgetting that go beyond a simple QueueUnbind:
//
//  1. Explicit QueueDelete removes the queue's bindings; the source auto-delete
//     exchange is cascade-forgotten because it now has no bindings.
//
//  2. ExchangeUnbind on an exchange-to-exchange binding cascade-forgets the source
//     auto-delete exchange when no bindings remain sourced from it.
//
//  3. Full chain outerExchange→innerExchange→queue with one consumer: cancelling
//     the last consumer cascade-forgets the queue, then innerExchange, then
//     outerExchange, verified both in the topology store and on the broker after
//     connection recovery.
//
//  4. ExchangeDelete of the destination exchange in an exchange-to-exchange binding
//     cascade-forgets the auto-delete source exchange when no other bindings remain
//     sourced from it.
func TestConnectionRecoveryAutoDeleteExchangeCascade(t *testing.T) {
	connectionName := "test-connection-recovery-auto-delete-exchange-cascade"

	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)
	conn, err := DialConfig(amqpURL, Config{
		Recovery:   &Recovery{},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	// -----------------------------------------------------------------------
	// Scenario 1 — QueueDelete cascades to auto-delete source exchange
	// -----------------------------------------------------------------------

	s1Exchange := "test_cascade_s1_exchange"
	if err := ch.ExchangeDeclare(s1Exchange, "direct", false, true, false, false, nil); err != nil {
		t.Fatalf("s1 ExchangeDeclare failed: %v", err)
	}
	defer func() { _ = ch.ExchangeDelete(s1Exchange, false, false) }()

	// Durable queue so that QueueDelete is an explicit action (not auto-delete lifecycle).
	s1Queue := "test_cascade_s1_queue"
	if _, err := ch.QueueDeclare(s1Queue, true, false, false, false, nil); err != nil {
		t.Fatalf("s1 QueueDeclare failed: %v", err)
	}
	if err := ch.QueueBind(s1Queue, "s1-key", s1Exchange, false, nil); err != nil {
		t.Fatalf("s1 QueueBind failed: %v", err)
	}

	// Verify exchange is tracked before the delete.
	if _, found := ch.TopologyConfiguration(true).Exchanges[s1Exchange]; !found {
		t.Fatalf("s1: expected exchange %q to be tracked before QueueDelete", s1Exchange)
	}

	// Explicitly delete the queue — this removes its binding and must cascade-forget
	// the now-binding-free auto-delete exchange.
	if _, err := ch.QueueDelete(s1Queue, false, false, false); err != nil {
		t.Fatalf("s1 QueueDelete failed: %v", err)
	}
	if _, found := ch.TopologyConfiguration(true).Exchanges[s1Exchange]; found {
		t.Fatalf("s1: auto-delete exchange %q must be forgotten after QueueDelete removed its last binding, but it is still tracked", s1Exchange)
	}
	t.Logf("Scenario 1 OK: exchange %q cascade-forgotten after QueueDelete", s1Exchange)

	// -----------------------------------------------------------------------
	// Scenario 2 — ExchangeUnbind (exchange-to-exchange) cascades to source
	// -----------------------------------------------------------------------

	s2Source := "test_cascade_s2_source_exchange"
	s2Dest := "test_cascade_s2_dest_exchange"
	if err := ch.ExchangeDeclare(s2Source, "fanout", false, true, false, false, nil); err != nil {
		t.Fatalf("s2 source ExchangeDeclare failed: %v", err)
	}
	defer func() { _ = ch.ExchangeDelete(s2Source, false, false) }()
	// Destination does not have to be auto-delete; only the source is checked.
	if err := ch.ExchangeDeclare(s2Dest, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("s2 dest ExchangeDeclare failed: %v", err)
	}
	defer func() { _ = ch.ExchangeDelete(s2Dest, false, false) }()

	if err := ch.ExchangeBind(s2Dest, "", s2Source, false, nil); err != nil {
		t.Fatalf("s2 ExchangeBind failed: %v", err)
	}

	// Verify source exchange is tracked before the unbind.
	if _, found := ch.TopologyConfiguration(true).Exchanges[s2Source]; !found {
		t.Fatalf("s2: expected source exchange %q to be tracked before ExchangeUnbind", s2Source)
	}

	if err := ch.ExchangeUnbind(s2Dest, "", s2Source, false, nil); err != nil {
		t.Fatalf("s2 ExchangeUnbind failed: %v", err)
	}
	if _, found := ch.TopologyConfiguration(true).Exchanges[s2Source]; found {
		t.Fatalf("s2: auto-delete source exchange %q must be forgotten after ExchangeUnbind removed its last binding, but it is still tracked", s2Source)
	}
	t.Logf("Scenario 2 OK: source exchange %q cascade-forgotten after ExchangeUnbind", s2Source)

	// -----------------------------------------------------------------------
	// Scenario 3 — full chain: outerExchange → innerExchange → queue → consumer
	//
	// Cancelling the last consumer must cascade-forget: queue → innerExchange →
	// outerExchange, and recovery must not re-declare any of them.
	// -----------------------------------------------------------------------

	outerExchange := "test_cascade_s3_outer_exchange"
	innerExchange := "test_cascade_s3_inner_exchange"
	s3Queue := "test_cascade_s3_queue"

	if err := ch.ExchangeDeclare(outerExchange, "fanout", false, true, false, false, nil); err != nil {
		t.Fatalf("s3 outerExchange ExchangeDeclare failed: %v", err)
	}
	defer func() { _ = ch.ExchangeDelete(outerExchange, false, false) }()

	if err := ch.ExchangeDeclare(innerExchange, "direct", false, true, false, false, nil); err != nil {
		t.Fatalf("s3 innerExchange ExchangeDeclare failed: %v", err)
	}
	defer func() { _ = ch.ExchangeDelete(innerExchange, false, false) }()

	// outerExchange → innerExchange
	if err := ch.ExchangeBind(innerExchange, "", outerExchange, false, nil); err != nil {
		t.Fatalf("s3 ExchangeBind (outer→inner) failed: %v", err)
	}

	// innerExchange → queue; exclusive+auto-delete satisfies RabbitMQ 3.12+.
	if _, err := ch.QueueDeclare(s3Queue, false, true, true, false, nil); err != nil {
		t.Fatalf("s3 QueueDeclare failed: %v", err)
	}
	if err := ch.QueueBind(s3Queue, "s3-key", innerExchange, false, nil); err != nil {
		t.Fatalf("s3 QueueBind (inner→queue) failed: %v", err)
	}

	if _, err := ch.Consume(s3Queue, "consumer-s3", true, false, false, false, nil); err != nil {
		t.Fatalf("s3 Consume failed: %v", err)
	}

	// Verify full chain is tracked before the cancel.
	topo := ch.TopologyConfiguration(true)
	for _, name := range []string{outerExchange, innerExchange} {
		if _, found := topo.Exchanges[name]; !found {
			t.Fatalf("s3: expected exchange %q to be tracked before consumer cancel", name)
		}
	}
	if _, found := topo.Queues[s3Queue]; !found {
		t.Fatalf("s3: expected queue %q to be tracked before consumer cancel", s3Queue)
	}

	// Cancel the only consumer — this must cascade-forget the queue, then
	// innerExchange (its only binding is gone), then outerExchange (its only
	// exchange-to-exchange binding is gone).
	if err := ch.Cancel("consumer-s3", false); err != nil {
		t.Fatalf("s3 Cancel failed: %v", err)
	}

	topo = ch.TopologyConfiguration(true)
	if _, found := topo.Queues[s3Queue]; found {
		t.Fatalf("s3: queue %q must be forgotten after last consumer cancel, but still tracked", s3Queue)
	}
	if _, found := topo.Exchanges[innerExchange]; found {
		t.Fatalf("s3: innerExchange %q must be cascade-forgotten after queue was forgotten, but still tracked", innerExchange)
	}
	if _, found := topo.Exchanges[outerExchange]; found {
		t.Fatalf("s3: outerExchange %q must be cascade-forgotten after innerExchange was forgotten, but still tracked", outerExchange)
	}
	t.Logf("Scenario 3 OK: full chain cascade-forgotten on last consumer cancel")

	// Verify on the broker after connection recovery: none of the chain entities
	// must be re-declared.  Use a surviving exclusive queue so we can confirm that
	// recovery actually ran before checking the absent entities.
	survivorQueue := "test_cascade_s3_survivor"
	if _, err := ch.QueueDeclare(survivorQueue, false, false, true, false, nil); err != nil {
		t.Fatalf("s3 survivor QueueDeclare failed: %v", err)
	}
	survivorMsgs, err := ch.Consume(survivorQueue, "consumer-s3-survivor", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("s3 survivor Consume failed: %v", err)
	}

	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)
	chanStateChanged := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateChanged)

	dropConnection(t, connectionName)
	waitForConnectionOpen(t, stateChanged)
	waitForChannelOpen(t, chanStateChanged)

	// Confirm recovery ran via the survivor queue.
	if err := ch.PublishWithContext(context.Background(), "", survivorQueue, false, false,
		Publishing{ContentType: "text/plain", Body: []byte("s3-probe")}); err != nil {
		t.Fatalf("s3 publish to survivor queue failed: %v", err)
	}
	select {
	case msg, ok := <-survivorMsgs:
		if !ok {
			t.Fatalf("s3 survivor consumer channel closed after recovery")
		}
		if string(msg.Body) != "s3-probe" {
			t.Fatalf("s3 survivor: expected 's3-probe', got %q", string(msg.Body))
		}
		t.Logf("s3 survivor received post-recovery message: %s", string(msg.Body))
	case <-time.After(10 * time.Second):
		t.Fatalf("s3: timeout waiting for post-recovery message on survivor queue")
	}

	// Each of the three cascade-forgotten entities must be absent on the broker.
	type passiveCheck struct {
		name string
		fn   func(*Channel) error
	}
	checks := []passiveCheck{
		{
			name: "queue " + s3Queue,
			fn: func(c *Channel) error {
				_, err := c.QueueDeclarePassive(s3Queue, false, true, true, false, nil)
				return err
			},
		},
		{
			name: "innerExchange " + innerExchange,
			fn: func(c *Channel) error {
				return c.ExchangeDeclarePassive(innerExchange, "direct", false, true, false, false, nil)
			},
		},
		{
			name: "outerExchange " + outerExchange,
			fn: func(c *Channel) error {
				return c.ExchangeDeclarePassive(outerExchange, "fanout", false, true, false, false, nil)
			},
		},
	}
	// Use a plain, non-recovering connection for these checks: each is expected
	// to close its channel with a 404, and running them on the recovering
	// `conn` would trigger the library's own per-channel auto-recovery
	// (Connection.watchChannel -> OnChannelClose -> Channel.Reconnect) for a
	// channel we're intentionally breaking, racing with the next check.
	verifyConn, err := DialConfig(amqpURL, Config{Locale: defaultLocale})
	if err != nil {
		t.Fatalf("s3 verification connection failed: %v", err)
	}
	defer verifyConn.Close()

	for _, ck := range checks {
		verifyCh, err := verifyConn.Channel()
		if err != nil {
			t.Fatalf("s3 verification channel failed: %v", err)
		}
		err = ck.fn(verifyCh)
		verifyCh.Close()
		if err == nil {
			t.Fatalf("s3: expected %s to be absent after recovery (cascade-forgotten), but it exists", ck.name)
		}
		amqpErr, ok := err.(*Error)
		if !ok || amqpErr.Code != NotFound {
			t.Fatalf("s3: expected NotFound (404) for absent %s, got: %v", ck.name, err)
		}
		t.Logf("s3: confirmed %s was NOT re-declared during recovery", ck.name)
	}

	// -----------------------------------------------------------------------
	// Scenario 4 — ExchangeDelete of destination cascades to auto-delete source
	//
	// Explicitly deleting the destination exchange of an exchange-to-exchange
	// binding must cascade-forget the auto-delete source exchange from the
	// topology store when no other bindings remain sourced from it.
	// -----------------------------------------------------------------------

	s4Source := "test_cascade_s4_source_exchange"
	s4Dest := "test_cascade_s4_dest_exchange"

	if err := ch.ExchangeDeclare(s4Source, "fanout", false, true, false, false, nil); err != nil {
		t.Fatalf("s4 source ExchangeDeclare failed: %v", err)
	}
	// s4Source is auto-delete: the broker removes it when its last binding is gone,
	// so no explicit cleanup defer is needed.
	if err := ch.ExchangeDeclare(s4Dest, "direct", false, false, false, false, nil); err != nil {
		t.Fatalf("s4 dest ExchangeDeclare failed: %v", err)
	}
	// s4Dest is deleted in the test body below; no defer needed.

	if err := ch.ExchangeBind(s4Dest, "", s4Source, false, nil); err != nil {
		t.Fatalf("s4 ExchangeBind (source→dest) failed: %v", err)
	}

	// Verify source exchange is tracked before the delete.
	if _, found := ch.TopologyConfiguration(true).Exchanges[s4Source]; !found {
		t.Fatalf("s4: expected source exchange %q to be tracked before ExchangeDelete of destination", s4Source)
	}

	// Explicitly delete the destination exchange. removeExchangeLocked collects
	// s4Source as a cascade candidate (it sourced a binding pointing to the deleted
	// exchange), and maybeDeleteRecordedAutoDeleteExchange must then forget it
	// because no bindings remain sourced from it.
	if err := ch.ExchangeDelete(s4Dest, false, false); err != nil {
		t.Fatalf("s4 ExchangeDelete dest failed: %v", err)
	}
	if _, found := ch.TopologyConfiguration(true).Exchanges[s4Source]; found {
		t.Fatalf("s4: auto-delete source exchange %q must be forgotten after ExchangeDelete removed its only e2e binding, but it is still tracked", s4Source)
	}
	t.Logf("Scenario 4 OK: source exchange %q cascade-forgotten after ExchangeDelete of destination %q", s4Source, s4Dest)
}

// TestConnectionRecoverySkipAndContinue tests that when a topology entity fails to
// recover, OnTopologyEntityError can skip it and recovery still completes successfully.
// The StateReconnecting→StateOpen transition must carry the skipped entities in
// SkippedTopologyEntities so the caller can observe what was not restored.
func TestConnectionRecoverySkipAndContinue(t *testing.T) {
	connectionName := "test-connection-recovery-skip-and-continue"
	properties := NewConnectionProperties()
	properties.SetClientConnectionName(connectionName)

	// 1. Declare topology: auto-delete exchange, durable queue (with x-max-length),
	//    a binding, and a consumer.
	conn, err := DialConfig(amqpURL, Config{
		Recovery: &Recovery{
			// Explicitly return true to skip the failed entity and continue recovery.
			OnTopologyEntityError: func(_ *Connection, e TopologyRecoveryEntity) bool {
				return true
			},
		},
		Locale:     defaultLocale,
		Properties: properties,
	})
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel creation failed: %v", err)
	}
	defer ch.Close()

	exchangeName := "test_skip_continue_ex"
	if err := ch.ExchangeDeclare(exchangeName, "direct", false, true, false, false, nil); err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}
	defer func() {
		if !conn.IsClosed() {
			_ = ch.ExchangeDelete(exchangeName, false, false)
		}
	}()

	queueName := "test_skip_continue_q"
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, Table{"x-max-length": int32(10)}); err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}
	defer func() {
		// ch may be closed after the conflicting-queue error during recovery; use a
		// fresh channel for cleanup so the queue is always removed.
		if conn.IsClosed() {
			return
		}
		cleanupCh, cerr := conn.Channel()
		if cerr != nil {
			return
		}
		defer cleanupCh.Close()
		_, _ = cleanupCh.QueueDelete(queueName, false, false, false)
	}()

	routingKey := "skip-continue-key"
	if err := ch.QueueBind(queueName, routingKey, exchangeName, false, nil); err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	msgs, err := ch.Consume(queueName, "skip-continue-consumer", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	_ = msgs

	// 2. Out-of-band: delete and redeclare the queue with a conflicting definition
	//    so that the client's recovery-time redeclare fails with PRECONDITION_FAILED.
	adminConn, err := DialConfig(amqpURL, Config{Locale: defaultLocale})
	if err != nil {
		t.Fatalf("admin DialConfig failed: %v", err)
	}
	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("admin Channel failed: %v", err)
	}
	if _, err := adminCh.QueueDelete(queueName, false, false, false); err != nil {
		t.Fatalf("admin QueueDelete failed: %v", err)
	}
	if _, err := adminCh.QueueDeclare(queueName, true, false, false, false, Table{"x-max-length": int32(99)}); err != nil {
		t.Fatalf("admin QueueDeclare (conflicting) failed: %v", err)
	}
	_ = adminCh.Close()
	_ = adminConn.Close()

	// 3. Register state change listener.
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)

	// 4. Drop the connection.
	dropConnection(t, connectionName)

	// 5. Wait for recovery; 6. Assert the open transition carries skipped entities.
	timeout := time.After(30 * time.Second)
	for {
		select {
		case sc := <-stateChanged:
			t.Logf("Connection state changed: %s", sc)
			if sc.To != StateOpen {
				continue
			}
			if len(sc.SkippedTopologyEntities) == 0 {
				t.Fatalf("Expected SkippedTopologyEntities to be non-empty on StateOpen after skip-and-continue, but it was nil/empty")
			}
			t.Logf("Recovery succeeded with %d skipped topology entity/entities:", len(sc.SkippedTopologyEntities))
			for _, e := range sc.SkippedTopologyEntities {
				t.Logf("  - %s %q on channel %d: %v", e.EntityType, e.EntityName, e.ChannelID, e.Err)
			}
			// Verify at least one skipped entity is the conflicting queue.
			foundQueue := false
			for _, e := range sc.SkippedTopologyEntities {
				if e.EntityType == TopologyEntityQueue && e.EntityName == queueName {
					foundQueue = true
				}
			}
			if !foundQueue {
				t.Fatalf("Expected a skipped entity for queue %q but it was not found in SkippedTopologyEntities: %+v",
					queueName, sc.SkippedTopologyEntities)
			}
			return
		case <-timeout:
			t.Fatalf("Timeout waiting for connection to recover to StateOpen with skipped topology entities")
		}
	}
}
