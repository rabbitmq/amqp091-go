package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Enable library logging
	amqp.Logger = log.Default()

	// 4. Register signal.NotifyContext with sigterm so that program exits gracefully on CTLR+C
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	url := "amqp://guest:guest@localhost:5672/"

	// 1. Create a connection with DialRecovery(url, nil) , i.e. default recovery configuration
	log.Printf("Dialing %s", url)
	conn, err := amqp.DialRecovery(url, nil)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	queueName := "recovery_test_queue"
	q, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	var wg sync.WaitGroup

	// 3. Consumer goroutine is continuosly consume message
	wg.Add(1)
	go func() {
		defer wg.Done()

		msgs, err := ch.Consume(
			q.Name,
			"recovery-consumer", // consumer tag
			true,                // auto-ack
			false,               // exclusive
			false,               // no-local
			false,               // no-wait
			nil,                 // args
		)
		if err != nil {
			log.Printf("Failed to register a consumer: %v", err)
			return
		}

		log.Println("Consumer started. Waiting for messages...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, stopping consumer")
				return
			case d, ok := <-msgs:
				if !ok {
					log.Println("Consumer channel closed")
					return
				}
				log.Printf("Received: %s", d.Body)
			}
		}
	}()

	// 2. Publisher goroutine will continuosly publish message, assume "Published Msg: 1" with increamental number.
	wg.Add(1)
	go func() {
		defer wg.Done()

		counter := 1
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		log.Println("Publisher started. Publishing messages...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, stopping publisher")
				return
			case <-ticker.C:
				body := fmt.Sprintf("Message %d", counter)
				err := ch.PublishWithContext(ctx,
					"",     // exchange
					q.Name, // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(body),
					})
				if err != nil {
					log.Printf("Failed to publish message %d: %v", counter, err)
				} else {
					log.Printf("Published: %s", body)
				}
				counter++
			}
		}
	}()

	log.Println("Running. Press CTRL+C to exit.")

	// Wait for goroutines to finish
	wg.Wait()
	log.Println("Shutting down gracefully.")
}
