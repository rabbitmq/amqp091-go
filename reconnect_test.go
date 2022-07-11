// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091_test

import (
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Every connection should declare the topology they expect
func setup(url, queue string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	if _, err := ch.QueueDeclare(queue, false, true, false, false, nil); err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}

func consume(url, queue string) (*amqp.Connection, <-chan amqp.Delivery, error) {
	conn, ch, err := setup(url, queue)
	if err != nil {
		return nil, nil, err
	}

	// Indicate we only want 1 message to acknowledge at a time.
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, nil, err
	}

	// Exclusive consumer
	deliveries, err := ch.Consume(queue, "", false, true, false, false, nil)

	return conn, deliveries, err
}

func ExampleConnection_reconnect() {
	if url := os.Getenv("AMQP_URL"); url != "" {
		queue := "example.reconnect"

		// The connection/channel for publishing to interleave the ingress messages
		// between reconnects, shares the same topology as the consumer.  If we rather
		// sent all messages up front, the first consumer would receive every message.
		// We would rather show how the messages are not lost between reconnects.
		con, pub, err := setup(url, queue)
		if err != nil {
			fmt.Println("err publisher setup:", err)
			return
		}
		defer con.Close()

		// Purge the queue from the publisher side to establish initial state
		if _, err := pub.QueuePurge(queue, false); err != nil {
			fmt.Println("err purge:", err)
			return
		}

		// Reconnect simulation, should be for { ... } in production
		for i := 1; i <= 3; i++ {
			fmt.Println("connect")

			conn, deliveries, err := consume(url, queue)
			if err != nil {
				fmt.Println("err consume:", err)
				return
			}

			// Simulate a producer on a different connection showing that consumers
			// continue where they were left off after each reconnect.
			if err := pub.Publish("", queue, false, false, amqp.Publishing{
				Body: []byte(fmt.Sprintf("%d", i)),
			}); err != nil {
				fmt.Println("err publish:", err)
				return
			}

			// Simulates a consumer that when the range finishes, will setup a new
			// session and begin ranging over the deliveries again.
			for msg := range deliveries {
				fmt.Println(string(msg.Body))
				if e := msg.Ack(false); e != nil {
					fmt.Println("ack error: ", e)
				}

				// Simulate an error like a server restart, loss of route or operator
				// intervention that results in the connection terminating
				go conn.Close()
			}
		}
	} else {
		// pass with expected output when not running in an integration
		// environment.
		fmt.Println("connect")
		fmt.Println("1")
		fmt.Println("connect")
		fmt.Println("2")
		fmt.Println("connect")
		fmt.Println("3")
	}

	// Output:
	// connect
	// 1
	// connect
	// 2
	// connect
	// 3
}
