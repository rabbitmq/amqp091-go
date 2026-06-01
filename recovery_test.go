// Copyright (c) 2026 Broadcom. All Rights Reserved.
// The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

//go:build integration

package amqp091

import (
	"context"
	"net"
	"net/url"
	"strings"
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
	SetLogger(testLogger{t: t})
	defer SetLogger(NullLogger{})

	// Create a connection with DialRecovery(url, nil)
	conn, err := DialRecovery(amqpURL, nil)
	if err != nil {
		t.Fatalf("DialRecovery failed: %v", err)
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

	queueName := "recovery_queue"
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
	err = ch.PublishWithContext(
		context.Background(),
		exchangeName,
		routingKey,
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello recovery 1"),
		},
	)
	if err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}

	// Consume message on the given channel
	msgs, err := ch.Consume(
		queueName,
		"",    // consumer tag
		true,  // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	select {
	case d, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed prematurely")
		}
		if string(d.Body) != "hello recovery 1" {
			t.Fatalf("Expected message 'hello recovery 1', got: %s", string(d.Body))
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for message 1")
	}

	// Register with connection for NotifyStateChange
	stateChanged := make(chan *StateChanged, 10)
	conn.NotifyStateChange(stateChanged)

	// Register with channel for NotifyStateChange
	chanStateChanged := make(chan *StateChanged, 10)
	ch.NotifyStateChange(chanStateChanged)

	// Call Http API to close the current connection
	dropConnection(t, conn)

	// Wait for connection to be open
	waitForConnectionOpen(t, stateChanged)

	// Verify channel state change notification is received and is reconnecting, followed by open
	waitForChannelOpen(t, chanStateChanged)

	// Verify Publish message on the given channel post-recovery.
	err = ch.PublishWithContext(
		context.Background(),
		exchangeName,
		routingKey,
		false,
		false,
		Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello recovery 2"),
		},
	)
	if err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	// Verify message is received on the given channel post-recovery.
	select {
	case d, ok := <-msgs:
		if !ok {
			t.Fatalf("Consume channel closed after recovery")
		}
		if string(d.Body) != "hello recovery 2" {
			t.Fatalf("Expected message 'hello recovery 2', got: %s", string(d.Body))
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for message 2 post-recovery")
	}
}

// TestConnectionRecoveryConsume tests the connection recovery for consume.
func TestConnectionRecoveryConsume(t *testing.T) {
	SetLogger(testLogger{t: t})
	defer SetLogger(NullLogger{})

	conn, err := DialRecovery(amqpURL, nil)
	if err != nil {
		t.Fatalf("DialRecovery failed: %v", err)
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
	dropConnection(t, conn)

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

func dropConnection(t *testing.T, conn *Connection) {
	localAddr := conn.LocalAddr().String()
	_, localPortStr, err := net.SplitHostPort(localAddr)
	if err != nil {
		t.Fatalf("SplitHostPort failed for localAddr %s: %v", localAddr, err)
	}

	var targetConnName string
	loopDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(loopDeadline) {
		conns, err := utils.Connections()
		if err != nil {
			t.Logf("Failure listing connections (will retry): %v", err)
			time.Sleep(time.Second)
			continue
		}

		for _, c := range conns {
			if strings.Contains(c.Name, ":"+localPortStr+" ->") {
				targetConnName = c.Name
				break
			}
		}

		if targetConnName != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if targetConnName == "" {
		conns, _ := utils.Connections()
		t.Fatalf("Could not find connection name for local address: %s (port: %s) in connections: %+v", localAddr, localPortStr, conns)
	}

	t.Logf("Dropping connection: %s", targetConnName)
	err = utils.DropConnection(url.PathEscape(targetConnName), "15672")
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
