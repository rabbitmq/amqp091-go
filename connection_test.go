// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build integration

package amqp091

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const rabbitmqctlEnvKey = "RABBITMQ_RABBITMQCTL_PATH"

func TestRequiredServerLocale(t *testing.T) {
	conn := integrationConnection(t, "AMQP 0-9-1 required server locale")
	t.Cleanup(func() { conn.Close() })
	requiredServerLocale := defaultLocale

	for _, locale := range conn.Locales {
		if locale == requiredServerLocale {
			return
		}
	}

	t.Fatalf("AMQP 0-9-1 server must support at least the %s locale, server sent the following locales: %#v", requiredServerLocale, conn.Locales)
}

func TestDefaultConnectionLocale(t *testing.T) {
	conn := integrationConnection(t, "client default locale")
	t.Cleanup(func() { conn.Close() })

	if conn.Config.Locale != defaultLocale {
		t.Fatalf("Expected default connection locale to be %s, is was: %s", defaultLocale, conn.Config.Locale)
	}
}

func TestChannelOpenOnAClosedConnectionFails(t *testing.T) {
	conn := integrationConnection(t, "channel on close")

	conn.Close()

	if _, err := conn.Channel(); err != ErrClosed {
		t.Fatalf("channel.open on a closed connection %#v is expected to fail", conn)
	}
}

// TestChannelOpenOnAClosedConnectionFails_ReleasesAllocatedChannel ensures the
// channel allocated is released if opening the channel fails.
func TestChannelOpenOnAClosedConnectionFails_ReleasesAllocatedChannel(t *testing.T) {
	conn := integrationConnection(t, "releases channel allocation")
	conn.Close()

	before := len(conn.channels)

	if _, err := conn.Channel(); err != ErrClosed {
		t.Fatalf("channel.open on a closed connection %#v is expected to fail", conn)
	}

	if len(conn.channels) != before {
		t.Fatalf("channel.open failed, but the allocated channel was not released")
	}
}

// TestRaceBetweenChannelAndConnectionClose ensures allocating a new channel
// does not race with shutting the connection down.
//
// See https://github.com/streadway/amqp/issues/251 - thanks to jmalloc for the
// test case.
func TestRaceBetweenChannelAndConnectionClose(t *testing.T) {
	defer time.AfterFunc(10*time.Second, func() { t.Fatalf("Close deadlock") }).Stop()

	conn := integrationConnection(t, "allocation/shutdown race")

	go conn.Close()
	for i := 0; i < 10; i++ {
		go func() {
			ch, err := conn.Channel()
			if err == nil {
				ch.Close()
			}
		}()
	}
}

// TestRaceBetweenChannelShutdownAndSend ensures closing a channel
// (channel.shutdown) does not race with calling channel.send() from any other
// goroutines.
//
// See https://github.com/streadway/amqp/pull/253#issuecomment-292464811 for
// more details - thanks to jmalloc again.
func TestRaceBetweenChannelShutdownAndSend(t *testing.T) {
	const concurrency = 10
	defer time.AfterFunc(10*time.Second, func() { t.Fatalf("Close deadlock") }).Stop()

	conn := integrationConnection(t, "channel close/send race")
	defer conn.Close()

	ch, _ := conn.Channel()
	go ch.Close()

	errs := make(chan error, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			// ch.Ack calls ch.send() internally.
			if err := ch.Ack(42, false); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Logf("[INFO] %#v (%s) of type %T", err, err, err)
		}
	}
}

func TestQueueDeclareOnAClosedConnectionFails(t *testing.T) {
	conn := integrationConnection(t, "queue declare on close")
	ch, _ := conn.Channel()

	conn.Close()

	if _, err := ch.QueueDeclare("an example", false, false, false, false, nil); err != ErrClosed {
		t.Fatalf("queue.declare on a closed connection %#v is expected to return ErrClosed, returned: %#v", conn, err)
	}
}

func TestConcurrentClose(t *testing.T) {
	const concurrency = 32

	conn := integrationConnection(t, "concurrent close")
	defer conn.Close()

	errs := make(chan error, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			err := conn.Close()

			if err == nil {
				t.Log("first concurrent close was successful")
				return
			}

			if err == ErrClosed {
				t.Log("later concurrent close were successful and returned ErrClosed")
				return
			}

			// BUG(st) is this really acceptable? we got a net.OpError before the
			// connection was marked as closed means a race condition between the
			// network connection and handshake state. It should be a package error
			// returned.
			if _, neterr := err.(*net.OpError); neterr {
				t.Logf("unknown net.OpError during close, ignoring: %+v", err)
				return
			}

			// A different/protocol error occurred indicating a race or missed condition
			if _, other := err.(*Error); other {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("Expected no error, or ErrClosed, or a net.OpError from conn.Close(), got %#v (%s) of type %T", err, err, err)
		}
	}
}

// TestPlaintextDialTLS ensures amqp:// connections succeed when using DialTLS.
func TestPlaintextDialTLS(t *testing.T) {
	uri, err := ParseURI(amqpURL)
	if err != nil {
		t.Fatalf("parse URI error: %s", err)
	}

	// We can only test when we have a plaintext listener
	if uri.Scheme != "amqp" {
		t.Skip("requires server listening for plaintext connections")
	}

	conn, err := DialTLS(uri.String(), &tls.Config{MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatalf("unexpected dial error, got %v", err)
	}
	conn.Close()
}

// TestIsClosed will test the public method IsClosed on a connection.
func TestIsClosed(t *testing.T) {
	conn := integrationConnection(t, "public IsClosed()")

	if conn.IsClosed() {
		t.Fatalf("connection expected to not be marked as closed")
	}

	conn.Close()

	if !conn.IsClosed() {
		t.Fatal("connection expected to be marked as closed")
	}
}

// TestChannelIsClosed will test the public method IsClosed on a channel.
func TestChannelIsClosed(t *testing.T) {
	conn := integrationConnection(t, "public channel.IsClosed()")
	t.Cleanup(func() { conn.Close() })
	ch, _ := conn.Channel()

	if ch.IsClosed() {
		t.Fatalf("channel expected to not be marked as closed")
	}

	ch.Close()

	if !ch.IsClosed() {
		t.Fatal("channel expected to be marked as closed")
	}
}

// TestReaderGoRoutineTerminatesWhenMsgIsProcessedDuringClose tests the issue
// described in https://github.com/rabbitmq/amqp091-go/issues/69.
func TestReaderGoRoutineTerminatesWhenMsgIsProcessedDuringClose(t *testing.T) {
	const routines = 10
	c := integrationConnection(t, t.Name())

	var wg sync.WaitGroup
	startSigCh := make(chan any)

	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			<-startSigCh

			err := c.Close()
			if err != nil {
				t.Logf("close failed in routine %d: %s", id, err.Error())
			}
		}(i)
	}
	close(startSigCh)

	t.Log("waiting for go-routines to terminate")
	wg.Wait()
}

func TestConnectionConfigPropertiesWithClientProvidedConnectionName(t *testing.T) {
	const expectedConnectionName = "amqp091-go-test"

	connectionProperties := NewConnectionProperties()
	connectionProperties.SetClientConnectionName(expectedConnectionName)

	currentConnectionName, ok := connectionProperties["connection_name"]
	if !ok {
		t.Fatal("Connection name was not set by Table.SetClientConnectionName")
	}
	if currentConnectionName != expectedConnectionName {
		t.Fatalf("Connection name is set to: %s. Expected: %s",
			currentConnectionName,
			expectedConnectionName)
	}
}

func TestNewConnectionProperties_HasDefaultProperties(t *testing.T) {
	expectedProductName := defaultProduct
	expectedPlatform := platform

	props := NewConnectionProperties()

	productName, ok := props["product"]
	if !ok {
		t.Fatal("Product name was not set by NewConnectionProperties")
	}
	if productName != expectedProductName {
		t.Fatalf("Product name is set to: %s. Expected: %s",
			productName,
			expectedProductName,
		)
	}

	platform, ok := props["platform"]
	if !ok {
		t.Fatal("Platform was not set by NewConnectionProperties")
	}
	if platform != expectedPlatform {
		t.Fatalf("Platform is set to: %s. Expected: %s",
			platform,
			expectedPlatform,
		)
	}

	versionUntyped, ok := props["version"]
	if !ok {
		t.Fatal("Version was not set by NewConnectionProperties")
	}

	version, ok := versionUntyped.(string)
	if !ok {
		t.Fatalf("Version in NewConnectionProperties should be string. Type given was: %T", versionUntyped)
	}

	// semver regexp as specified by https://semver.org/
	semverRegexp := regexp.MustCompile(`^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)

	if !semverRegexp.MatchString(version) {
		t.Fatalf("Version in NewConnectionProperties is not a valid semver value: %s", version)
	}
}

// Connection and channels should be closeable when a memory alarm is active.
// https://github.com/rabbitmq/amqp091-go/issues/178
func TestConnection_Close_WhenMemoryAlarmIsActive(t *testing.T) {
	err := rabbitmqctl(t, "set_vm_memory_high_watermark", "0.0001")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = rabbitmqctl(t, "set_vm_memory_high_watermark", "0.4")

		// The broker does not lift the memory alarm instantaneously after the
		// watermark is reset, so retry until a connection/channel can be
		// established rather than flaking on a transient nil channel.
		var conn *Connection
		var ch *Channel
		deadline := time.Now().Add(time.Second * 10)
		for time.Now().Before(deadline) {
			conn, ch = integrationQueue(t, t.Name())
			if conn != nil && ch != nil {
				break
			}
			time.Sleep(time.Millisecond * 500)
		}
		if conn == nil || ch == nil {
			t.Fatal("timed out waiting for broker to accept connections after clearing memory alarm")
		}

		integrationQueueDelete(t, ch, t.Name())
		_ = ch.Close()
		_ = conn.Close()
	})

	conn, ch := integrationQueue(t, t.Name())

	go func() {
		// simulate a producer
		// required to block the connection
		_ = ch.PublishWithContext(context.Background(), "", t.Name(), false, false, Publishing{
			Body: []byte("this is a test"),
		})
	}()
	<-time.After(time.Second * 1)

	err = conn.CloseDeadline(time.Now().Add(time.Second * 2))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !conn.IsClosed() {
		t.Fatal("expected connection to be closed")
	}
}

// A publisher blocked by a memory alarm stalls in a socket write while the
// broker keeps sending heartbeats. The connection must outlive the alarm.
func TestConnection_SurvivesMemoryAlarmWithStalledPublisher(t *testing.T) {
	clearAlarm := func() { _ = rabbitmqctl(t, "set_vm_memory_high_watermark", "0.4") }
	if err := rabbitmqctl(t, "set_vm_memory_high_watermark", "0.0001"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clearAlarm)

	conf := defaultConfig()
	conf.Heartbeat = time.Second
	conn, err := DialConfig(amqpURL, conf)
	if err != nil {
		t.Fatalf("cannot dial integration server: %s", err)
	}
	closes := conn.NotifyClose(make(chan *Error, 1))
	blocks := conn.NotifyBlocked(make(chan Blocking, 16))
	blocked := false
	t.Cleanup(func() {
		clearAlarm()
		// On failure, give the broker time to lift the alarm before the next test.
		timeout := time.After(10 * time.Second)
		for blocked {
			select {
			case b, ok := <-blocks:
				blocked = ok && b.Active
			case <-timeout:
				blocked = false
			}
		}
		_ = conn.CloseDeadline(time.Now().Add(2 * time.Second))
	})
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %v", err)
	}

	awaitBlocking := func(active bool) {
		t.Helper()
		timeout := time.After(30 * time.Second)
		for {
			select {
			case b := <-blocks:
				blocked = b.Active
				if b.Active == active {
					return
				}
			case err := <-closes:
				t.Fatalf("connection closed waiting for blocked=%t: %v", active, err)
			case <-timeout:
				t.Fatalf("timed out waiting for blocked=%t", active)
			}
		}
	}

	// The messages are unroutable, and large enough to fill the socket buffers
	// once the broker stops reading.
	stop := make(chan struct{})
	published := publishInBackground(ch, make([]byte, 1<<20), stop)
	awaitBlocking(true)

	// The heartbeat timeout is 1.5s, so the connection would be long gone.
	select {
	case err := <-closes:
		t.Fatalf("connection closed while blocked by the memory alarm: %v", err)
	case err := <-published:
		t.Fatalf("publisher returned instead of stalling: %v", err)
	case <-time.After(6 * time.Second):
	}

	clearAlarm()
	awaitBlocking(false)
	close(stop)
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("publish failed after the alarm cleared: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("publisher still stalled after the alarm cleared")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("connection is not usable after the alarm: %v", err)
	}
}

func rabbitmqctl(t *testing.T, args ...string) error {
	t.Helper()

	rabbitmqctlPath, found := os.LookupEnv(rabbitmqctlEnvKey)
	if !found {
		t.Skipf("variable for %s for rabbitmqctl not found, skipping", rabbitmqctlEnvKey)
	}

	var cmd *exec.Cmd
	if strings.HasPrefix(rabbitmqctlPath, "DOCKER:") {
		containerName := strings.Split(rabbitmqctlPath, ":")[1]
		cmd = exec.Command("docker", "exec", containerName, "rabbitmqctl")
		cmd.Args = append(cmd.Args, args...)
	} else {
		cmd = exec.Command(rabbitmqctlPath, args...)
	}

	return cmd.Run()
}
