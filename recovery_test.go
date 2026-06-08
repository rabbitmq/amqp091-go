// Copyright (c) 2026 Broadcom. All Rights Reserved.
// The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

//go:build integration

package amqp091

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go/test/utils"
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
			if _, ok := sc.To.(*StateReconnecting); ok {
				connReconnectingSeen = true
			}
			if _, ok := sc.To.(*StateOpen); ok {
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
			if _, ok := sc.To.(*StateReconnecting); ok {
				chanReconnectingSeen = true
			}
			if _, ok := sc.To.(*StateOpen); ok {
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
			if _, ok := sc.To.(*StateClosed); ok {
				chanClosedSeen = true
			}
			if _, ok := sc.To.(*StateReconnecting); ok {
				chanReconnectingSeen = true
			}
			if _, ok := sc.To.(*StateOpen); ok {
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
