// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testHeartbeat = time.Second
	// The client gives up on the broker after 1.5 heartbeats without traffic.
	testReadTimeout = 3 * testHeartbeat / 2
	// How long a healthy connection must outlive a stall to prove it survives.
	testSurvival = 2 * testReadTimeout
	// How long the client may take to act on a heartbeat timeout.
	testDetection = 4 * testReadTimeout
)

// stallConn is a TCP connection whose writes block on demand, like a socket
// whose peer applies TCP back-pressure. Closing it fails the blocked writes.
type stallConn struct {
	net.Conn
	stall   chan struct{}
	resume  chan struct{}
	blocked chan struct{}
	closing chan struct{}
	closed  atomic.Bool
}

func newStallConn(conn net.Conn) *stallConn {
	return &stallConn{
		Conn:    conn,
		stall:   make(chan struct{}),
		resume:  make(chan struct{}),
		blocked: make(chan struct{}, 1),
		closing: make(chan struct{}),
	}
}

func (c *stallConn) Write(p []byte) (int, error) {
	select {
	case <-c.stall:
		select {
		case c.blocked <- struct{}{}:
		default:
		}
		select {
		case <-c.resume:
		case <-c.closing:
			return 0, net.ErrClosed
		}
	default:
	}
	return c.Conn.Write(p)
}

func (c *stallConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		close(c.closing)
	}
	return c.Conn.Close()
}

// openTCPSession opens a Connection to a scripted broker over a real TCP
// socket, as the in-memory pipes of newSession have no deadlines. The broker
// function runs once the connection is open and must return when closing is
// closed or the socket fails.
func openTCPSession(t *testing.T, broker func(srv *server, closing <-chan struct{})) (*Connection, *stallConn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	closing := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		srv := newServer(t, conn, nil)
		srv.connectionOpen()
		broker(srv, closing)
	}()

	tcp, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := newStallConn(tcp)
	t.Cleanup(func() {
		close(closing)
		_ = client.Close()
		_ = ln.Close()
		<-done
	})

	config := defaultConfig()
	config.Heartbeat = testHeartbeat
	c, err := Open(client, config)
	if err != nil {
		t.Fatalf("could not open connection: %v", err)
	}

	return c, client
}

// awaitMethod discards client frames until one carries a method of type M and
// reports false if the socket fails first.
func awaitMethod[M message](srv *server) bool {
	for {
		f, err := srv.r.ReadFrame()
		if err != nil {
			return false
		}
		if mf, ok := f.(*methodFrame); ok {
			if _, ok := mf.Method.(M); ok {
				return true
			}
		}
	}
}

func (t *server) reply(channel uint16, m message) bool {
	return t.w.WriteFrame(&methodFrame{ChannelId: channel, Method: m}) == nil
}

func (t *server) acceptChannel() bool {
	return awaitMethod[*channelOpen](t) && t.reply(1, &channelOpenOk{})
}

// serveAlive sends heartbeats, whatever the client does, until the client
// closes the connection.
func (t *server) serveAlive(closing <-chan struct{}) {
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(testHeartbeat / 10)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-closing:
				return
			case <-ticker.C:
				if t.w.WriteFrame(&heartbeatFrame{}) != nil {
					return
				}
			}
		}
	}()

	closeRequested := awaitMethod[*connectionClose](t)
	close(stop)
	<-stopped
	if closeRequested {
		t.reply(0, &connectionCloseOk{})
	}
}

// publishInBackground publishes body until stop is closed and reports the
// first publish error, or nil, on the returned channel.
func publishInBackground(ch *Channel, body []byte, stop <-chan struct{}) <-chan error {
	result := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				result <- nil
				return
			default:
			}
			err := ch.PublishWithContext(context.Background(), "", "q", false, false, Publishing{Body: body})
			if err != nil {
				result <- err
				return
			}
		}
	}()
	return result
}

func awaitStalledWrite(t *testing.T, client *stallConn) {
	t.Helper()
	select {
	case <-client.blocked:
	case <-time.After(testDetection):
		t.Fatalf("no write stalled on the socket")
	}
}

// A broker under a resource alarm stops reading from publishing connections
// but keeps sending heartbeats. The stalled publisher must not get the
// connection declared dead.
func TestHeartbeatSurvivesStalledPublisher(t *testing.T) {
	c, client := openTCPSession(t, func(srv *server, closing <-chan struct{}) {
		if srv.acceptChannel() {
			srv.serveAlive(closing)
		}
	})

	closes := c.NotifyClose(make(chan *Error, 1))
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %v", err)
	}

	close(client.stall)
	stop := make(chan struct{})
	published := publishInBackground(ch, []byte("payload"), stop)
	awaitStalledWrite(t, client)

	select {
	case err := <-closes:
		t.Fatalf("connection closed while the broker was sending heartbeats: %v", err)
	case err := <-published:
		t.Fatalf("publisher returned instead of stalling: %v", err)
	case <-time.After(testSurvival):
	}

	close(stop)
	close(client.resume)
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("publish failed after the stall ended: %v", err)
		}
	case <-time.After(testDetection):
		t.Fatalf("publisher still stalled after the socket resumed")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("connection is not usable after the stall: %v", err)
	}
}

// When the broker dies while a publisher is stalled in a socket write, the
// heartbeat timeout must still tear the connection down and fail the publish.
func TestHeartbeatTimeoutFreesStalledPublisher(t *testing.T) {
	c, client := openTCPSession(t, func(srv *server, closing <-chan struct{}) {
		if srv.acceptChannel() {
			<-closing
		}
	})

	closes := c.NotifyClose(make(chan *Error, 1))
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %v", err)
	}
	channelCloses := ch.NotifyClose(make(chan *Error, 1))

	close(client.stall)
	published := publishInBackground(ch, []byte("payload"), make(chan struct{}))
	awaitStalledWrite(t, client)

	timeout := time.After(testDetection)
	select {
	case err := <-closes:
		if err == nil {
			t.Fatalf("expected the heartbeat timeout as the close reason")
		}
	case <-timeout:
		t.Fatalf("connection not closed after the broker went silent")
	}
	select {
	case err := <-published:
		if err == nil {
			t.Fatalf("expected the stalled publish to fail")
		}
	case <-timeout:
		t.Fatalf("publisher still stalled after the heartbeat timeout")
	}
	select {
	case <-channelCloses:
	case <-timeout:
		t.Fatalf("channel not shut down after the heartbeat timeout")
	}
}

// The read deadline must be in force from the moment the connection is open,
// even if the broker never sends another frame.
func TestHeartbeatTimeoutDetectsSilentBroker(t *testing.T) {
	c, _ := openTCPSession(t, func(_ *server, closing <-chan struct{}) {
		<-closing
	})

	select {
	case err := <-c.NotifyClose(make(chan *Error, 1)):
		if err == nil {
			t.Fatalf("expected the heartbeat timeout as the close reason")
		}
	case <-time.After(testDetection):
		t.Fatalf("connection not closed after the broker went silent")
	}
}

// A listener that is slow to take a notification holds up the reader. The
// time spent there must not count against a broker that kept sending
// heartbeats.
func TestHeartbeatSurvivesSlowListener(t *testing.T) {
	deliver := make(chan struct{})
	c, _ := openTCPSession(t, func(srv *server, closing <-chan struct{}) {
		if !srv.acceptChannel() {
			return
		}
		select {
		case <-deliver:
		case <-closing:
			return
		}
		srv.send(1, &basicReturn{ReplyCode: NoRoute, ReplyText: "NO_ROUTE", Body: []byte("payload")})
		srv.serveAlive(closing)
	})

	closes := c.NotifyClose(make(chan *Error, 1))
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %v", err)
	}
	returns := ch.NotifyReturn(make(chan Return))
	close(deliver)

	select {
	case err := <-closes:
		t.Fatalf("connection closed while the listener was slow: %v", err)
	case <-time.After(testSurvival):
	}

	select {
	case <-returns:
	case <-time.After(testDetection):
		t.Fatalf("return not delivered")
	}

	// The reader is back on the socket, where a stale deadline fails at once.
	select {
	case err := <-closes:
		t.Fatalf("connection closed once the listener caught up: %v", err)
	case <-time.After(testHeartbeat):
	}

	if err := c.Close(); err != nil {
		t.Fatalf("connection is not usable after the slow listener: %v", err)
	}
}

// The dial deadline bounds the whole handshake. The heartbeat timeout must
// not replace it before the connection is open.
func TestHandshakeDeadlineNotExtendedByHeartbeat(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	closing := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		srv := newServer(t, conn, nil)
		srv.expectAMQP()
		srv.connectionStart()
		srv.connectionTune()
		srv.recv(0, &connectionOpen{})
		// Traffic makes the reader come back for another read.
		_ = srv.w.WriteFrame(&heartbeatFrame{})
		<-closing
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		close(closing)
		_ = client.Close()
		_ = ln.Close()
		<-done
	})

	start := time.Now()
	if err := client.SetDeadline(start.Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	config := defaultConfig()
	config.Heartbeat = 10 * time.Second
	if _, err := Open(client, config); err == nil {
		t.Fatalf("expected the handshake to time out")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("handshake took %v, the heartbeat timeout replaced the dial deadline", elapsed)
	}
}
