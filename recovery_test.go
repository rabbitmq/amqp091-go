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

type testLogger struct {
	t *testing.T
}

func (l testLogger) Printf(format string, v ...any) {
	l.t.Logf("[lib] "+format, v...)
}

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
