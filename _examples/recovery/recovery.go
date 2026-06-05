package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var url = flag.String("url", "amqp://guest:guest@localhost:5672/", "AMQP URL")

func init() {
	flag.Parse()
}

func main() {
	// Enable library logging
	amqp.Logger = log.Default()

	// Register signal.NotifyContext with sigterm so that program exits gracefully on CTLR+C
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Create a connection with DialRecovery(url, nil) , i.e. default recovery configuration
	log.Printf("Dialing %s", *url)
	conn, err := amqp.DialRecovery(*url, nil)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	signalBlock := sync.Cond{L: &sync.Mutex{}}
	var isReconnecting bool
	stateChanged := make(chan *amqp.StateChanged, 1)
	conn.NotifyStateChange(stateChanged)

	go func(ch chan *amqp.StateChanged) {
		for statusChanged := range ch {
			log.Printf("[connection] Status changed: %v", statusChanged)
			switch statusChanged.To.(type) {
			case *amqp.StateOpen:
				signalBlock.L.Lock()
				isReconnecting = false
				signalBlock.Broadcast()
				signalBlock.L.Unlock()
			case *amqp.StateReconnecting:
				log.Printf("[connection] Reconnecting to the AMQP server")
				signalBlock.L.Lock()
				isReconnecting = true
				signalBlock.L.Unlock()
			case *amqp.StateClosed:
				stateClosed := statusChanged.To.(*amqp.StateClosed)
				if stateClosed.GetError() != nil {
					log.Printf("[connection] Connection closed with error: %v", stateClosed.GetError())
				} else {
					log.Printf("[connection] Connection closed normally")
				}
				signalBlock.L.Lock()
				isReconnecting = false
				signalBlock.Broadcast()
				signalBlock.L.Unlock()
				cancel()
			}
		}
	}(stateChanged)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	chanStateChanged := make(chan *amqp.StateChanged, 1)
	ch.NotifyStateChange(chanStateChanged)

	go func(c chan *amqp.StateChanged) {
		for statusChanged := range c {
			log.Printf("[channel] Status changed: %v", statusChanged)
		}
	}(chanStateChanged)

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

	// Consumer goroutine will continuously consume message
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
					// Note: Consume channel does not close during automatic recovery.
					// It simply blocks until recovery is complete and new messages arrive.
					// If it does close, it means recovery failed or was disabled.
					log.Println("Consumer channel closed")
					return
				}
				log.Printf("Received: %s", d.Body)
			}
		}
	}()

	// Publisher goroutine will continuously publish message
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
					log.Println("Publisher is blocked, waiting for recovery...")
					time.Sleep(10 * time.Millisecond) // Let the notifier run to update the state
					signalBlock.L.Lock()
					for isReconnecting { // Avoid spurious wakeups
						signalBlock.Wait()
					}
					signalBlock.L.Unlock()
					log.Println("Publisher is unblocked")
					// Retry publishing the same message
					continue
				} else {
					log.Printf("Published: %s", body)
				}
				counter++
			}
		}
	}()

	// Stats will continuously monitor the number of active goroutines every 1 minute
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		log.Println("Stats started. Monitoring goroutines...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, stopping stats")
				return
			case <-ticker.C:
				log.Printf("[stats] Goroutines: %d", runtime.NumGoroutine())
			}
		}
	}()

	log.Println("Running. Press CTRL+C to exit.")

	// Wait for goroutines to finish
	wg.Wait()
	log.Println("Shutting down gracefully.")
}
