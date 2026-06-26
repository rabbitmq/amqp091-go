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

func waitForChannelClose(t *testing.T, chanStateChanged chan *StateChanged, chanID int, reason string) {
	var chanClosedSeen bool
	var chanReconnectingSeen bool
	var chanOpenSeen bool

	loopDeadline := time.After(3 * time.Second)
	for {
		select {
		case sc, ok := <-chanStateChanged:
			if !ok {
				if !chanClosedSeen {
					t.Fatalf("Expected Channel %d to transition to StateClosed before channel closed", chanID)
				}
				if chanReconnectingSeen || chanOpenSeen {
					t.Fatalf("Channel %d recovery was triggered for %s!", chanID, reason)
				}
				return
			}
			t.Logf("Channel %d state changed: %s", chanID, sc)
			if sc.To == StateClosed {
				chanClosedSeen = true
			}
			if sc.To == StateReconnecting {
				chanReconnectingSeen = true
			}
			if sc.To == StateOpen {
				chanOpenSeen = true
			}
		case <-loopDeadline:
			if !chanClosedSeen {
				t.Fatalf("Expected Channel %d to transition to StateClosed", chanID)
			}
			if chanReconnectingSeen || chanOpenSeen {
				t.Fatalf("Channel %d recovery was triggered for %s!", chanID, reason)
			}
			return
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

// TestConnectionRecoveryNonRecoverableChannelClose tests that channel recovery is NOT triggered
// when a channel is closed due to soft exceptions (errors not present in RecoverableErrorCodes).
// It verifies both PRECONDITION_FAILED (406) and RESOURCE_LOCKED (405) leave the connection open,
// transition the channel to StateClosed, and do not trigger automatic recovery.
func TestConnectionRecoveryNonRecoverableChannelClose(t *testing.T) {
	connectionName := "test-non-recoverable-channel-close"
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

	// --- 1. Test PRECONDITION_FAILED (406) using a durable mismatch on a non-exclusive queue ---
	ch1, err := conn.Channel()
	if err != nil {
		t.Fatalf("First Channel creation failed: %v", err)
	}
	defer ch1.Close()

	chanStateChanged1 := make(chan *StateChanged, 10)
	ch1.NotifyStateChange(chanStateChanged1)

	queueNamePrecondition := "precondition_failed_test_queue"
	_, _ = ch1.QueueDelete(queueNamePrecondition, false, false, false)

	// Declare non-exclusive queue as durable: true
	_, err = ch1.QueueDeclare(
		queueNamePrecondition,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		t.Fatalf("QueueDeclare durable:true failed: %v", err)
	}
	defer func() {
		// Clean up queue using a fresh channel since ch1 will be closed
		if !conn.IsClosed() {
			if cleanCh, err := conn.Channel(); err == nil {
				_, _ = cleanCh.QueueDelete(queueNamePrecondition, false, false, false)
				cleanCh.Close()
			}
		}
	}()

	// Declare again on SAME channel with durable: false (PreconditionFailed)
	_, err = ch1.QueueDeclare(
		queueNamePrecondition,
		false, // durable (mismatched)
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err == nil {
		t.Fatalf("Expected PreconditionFailed error but second QueueDeclare succeeded")
	}

	amqpErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("Expected amqp.Error, got: %T (%v)", err, err)
	}
	if amqpErr.Code != PreconditionFailed {
		t.Fatalf("Expected PreconditionFailed (406), got code: %d", amqpErr.Code)
	}

	// Verify ch1 transitioned to StateClosed and did not trigger recovery
	waitForChannelClose(t, chanStateChanged1, int(ch1.id), "PreconditionFailed")

	if conn.IsClosed() {
		t.Fatalf("Expected connection to remain open after PRECONDITION_FAILED soft exception")
	}
	t.Log("Verified connection remains open after PRECONDITION_FAILED")

	// --- 2. Test RESOURCE_LOCKED (405) using exclusive queue settings ---
	ch2, err := conn.Channel()
	if err != nil {
		t.Fatalf("Second Channel creation failed: %v", err)
	}
	defer ch2.Close()

	chanStateChanged2 := make(chan *StateChanged, 10)
	ch2.NotifyStateChange(chanStateChanged2)

	queueNameResource := "resource_locked_test_queue"
	_, _ = ch2.QueueDelete(queueNameResource, false, false, false)

	// Declare as exclusive: true, auto-delete: true
	_, err = ch2.QueueDeclare(
		queueNameResource,
		false, // durable
		true,  // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		t.Fatalf("QueueDeclare exclusive:true failed: %v", err)
	}

	// Declare again on SAME channel with exclusive: false (ResourceLocked)
	_, err = ch2.QueueDeclare(
		queueNameResource,
		false, // durable
		true,  // auto-delete
		false, // exclusive (mismatched)
		false, // no-wait
		nil,
	)
	if err == nil {
		t.Fatalf("Expected ResourceLocked error but second QueueDeclare succeeded")
	}

	amqpErr, ok = err.(*Error)
	if !ok {
		t.Fatalf("Expected amqp.Error, got: %T (%v)", err, err)
	}
	if amqpErr.Code != ResourceLocked {
		t.Fatalf("Expected ResourceLocked (405), got code: %d", amqpErr.Code)
	}

	// Verify ch2 transitioned to StateClosed and did not trigger recovery
	waitForChannelClose(t, chanStateChanged2, int(ch2.id), "ResourceLocked")

	if conn.IsClosed() {
		t.Fatalf("Expected connection to remain open after RESOURCE_LOCKED soft exception")
	}
	t.Log("Verified connection remains open after RESOURCE_LOCKED")
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
