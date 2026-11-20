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
	return openTCPSessionWithHeartbeat(t, testHeartbeat, broker)
}

func openTCPSessionWithHeartbeat(t *testing.T, heartbeat time.Duration, broker func(srv *server, closing <-chan struct{})) (*Connection, *stallConn) {
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
	config.Heartbeat = heartbeat
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

// publishInBackground publishes body until stop is closed. It reports the
// first publish error, or nil, on the returned channel, and counts the
// completed publishes.
func publishInBackground(ch *Channel, body []byte, stop <-chan struct{}) (<-chan error, *atomic.Int64) {
	result := make(chan error, 1)
	published := new(atomic.Int64)
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
			published.Add(1)
		}
	}()
	return result, published
}

func awaitStalledWrite(t *testing.T, client *stallConn) {
	t.Helper()
	select {
	case <-client.blocked:
	case <-time.After(testDetection):
		t.Fatalf("no write stalled on the socket")
	}
}

// stallPublisher stalls the socket writes and returns once a publisher is
// stuck on them while holding Channel.m.
func stallPublisher(t *testing.T, client *stallConn, ch *Channel) <-chan error {
	t.Helper()

	close(client.stall)
	published, _ := publishInBackground(ch, []byte("payload"), make(chan struct{}))
	awaitStalledWrite(t, client)

	deadline := time.Now().Add(testDetection)
	for ch.m.TryLock() {
		ch.m.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("publisher never held the channel")
		}
		time.Sleep(time.Millisecond)
	}
	return published
}

func awaitClose(t *testing.T, closes <-chan *Error) *Error {
	t.Helper()
	select {
	case err := <-closes:
		if err == nil {
			t.Fatalf("expected a close reason")
		}
		return err
	case <-time.After(testDetection):
		t.Fatalf("connection not closed")
		return nil
	}
}

// A broker under a resource alarm stops reading from publishing connections
// but keeps sending heartbeats. The stalled publisher must not get the
// connection declared dead.
func TestHeartbeatSurvivesStalledPublisher(t *testing.T) {
	t.Parallel()

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
	published, _ := publishInBackground(ch, []byte("payload"), stop)
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
	t.Parallel()

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

	published := stallPublisher(t, client, ch)

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
	t.Parallel()

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
	t.Parallel()

	// The reader gives up on a listener after notifyTimeout.
	if notifyTimeout <= testSurvival {
		t.Fatalf("notifyTimeout %v is too short to keep the reader busy for %v", notifyTimeout, testSurvival)
	}

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

// A reply to an RPC sent before the stall must reach its caller, and must not
// keep the reader from noticing that the broker died afterwards.
func TestHeartbeatTimeoutNotDelayedByReplyOnStalledChannel(t *testing.T) {
	t.Parallel()

	declared := make(chan struct{})
	reply := make(chan struct{})
	c, client := openTCPSession(t, func(srv *server, closing <-chan struct{}) {
		if !srv.acceptChannel() || !awaitMethod[*queueDeclare](srv) {
			return
		}
		close(declared)
		select {
		case <-reply:
		case <-closing:
			return
		}
		srv.reply(1, &queueDeclareOk{Queue: "q"})
		<-closing
	})

	closes := c.NotifyClose(make(chan *Error, 1))
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %v", err)
	}

	declare := make(chan error, 1)
	go func() {
		_, err := ch.QueueDeclare("q", false, false, false, false, nil)
		declare <- err
	}()
	select {
	case <-declared:
	case <-time.After(testDetection):
		t.Fatalf("broker never received queue.declare")
	}

	stallPublisher(t, client, ch)
	close(reply)

	select {
	case err := <-declare:
		if err != nil {
			t.Fatalf("queue.declare failed although the broker replied: %v", err)
		}
	case <-time.After(testDetection):
		t.Fatalf("reply not delivered while a publisher was stalled")
	}
	if err := awaitClose(t, closes); err.Server {
		t.Fatalf("expected the heartbeat timeout as the close reason, got: %v", err)
	}
}

// A connection.close from the broker must be acted on even if close-ok can
// not be written.
func TestServerCloseNotDelayedByStalledPublisher(t *testing.T) {
	t.Parallel()

	closeNow := make(chan struct{})
	c, client := openTCPSession(t, func(srv *server, closing <-chan struct{}) {
		if !srv.acceptChannel() {
			return
		}
		select {
		case <-closeNow:
		case <-closing:
			return
		}
		srv.send(0, &connectionClose{ReplyCode: ConnectionForced, ReplyText: "CONNECTION_FORCED"})
		<-closing
	})

	closes := c.NotifyClose(make(chan *Error, 1))
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %v", err)
	}

	published := stallPublisher(t, client, ch)
	close(closeNow)

	if err := awaitClose(t, closes); err.Code != ConnectionForced {
		t.Fatalf("expected the broker's close reason, got: %v", err)
	}
	select {
	case err := <-published:
		if err == nil {
			t.Fatalf("expected the stalled publish to fail")
		}
	case <-time.After(testDetection):
		t.Fatalf("publisher still stalled after the broker closed the connection")
	}
}

// A channel.close for the stalled channel must not keep the reader from the
// frames that follow it. The close completes once the stall ends.
func TestReaderNotBlockedByCloseOfStalledChannel(t *testing.T) {
	t.Parallel()

	closeNow := make(chan struct{})
	c, client := openTCPSession(t, func(srv *server, closing <-chan struct{}) {
		if !srv.acceptChannel() {
			return
		}
		select {
		case <-closeNow:
		case <-closing:
			return
		}
		srv.send(1, &channelClose{ReplyCode: PreconditionFailed, ReplyText: "PRECONDITION_FAILED"})
		srv.send(0, &connectionBlocked{Reason: "after the close"})
		srv.serveAlive(closing)
	})

	closes := c.NotifyClose(make(chan *Error, 1))
	blocks := c.NotifyBlocked(make(chan Blocking, 1))
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %v", err)
	}
	channelCloses := ch.NotifyClose(make(chan *Error, 1))

	stallPublisher(t, client, ch)
	close(closeNow)

	select {
	case <-blocks:
	case err := <-closes:
		t.Fatalf("connection closed while the broker was sending heartbeats: %v", err)
	case <-time.After(testDetection):
		t.Fatalf("reader stuck behind the stalled publisher")
	}

	close(client.resume)
	select {
	case err := <-channelCloses:
		if err == nil || err.Code != PreconditionFailed {
			t.Fatalf("expected the broker's close reason, got: %v", err)
		}
	case <-time.After(testDetection):
		t.Fatalf("channel not closed after the stall ended")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("connection is not usable after the stall: %v", err)
	}
}

// The deadline given to CloseDeadline must hold even if the broker keeps
// sending frames instead of close-ok.
func TestCloseDeadlineNotExtendedByTraffic(t *testing.T) {
	t.Parallel()

	// A heartbeat long enough that no client heartbeat is due during the test.
	c, _ := openTCPSessionWithHeartbeat(t, 4*time.Second, func(srv *server, closing <-chan struct{}) {
		if !awaitMethod[*connectionClose](srv) {
			return
		}
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-closing:
				return
			case <-ticker.C:
				if srv.w.WriteFrame(&heartbeatFrame{}) != nil {
					return
				}
			}
		}
	})

	start := time.Now()
	if err := c.CloseDeadline(start.Add(300 * time.Millisecond)); err == nil {
		t.Fatalf("expected the deadline to fail the close")
	}
	if elapsed := time.Since(start); elapsed > 1200*time.Millisecond {
		t.Fatalf("close took %v, traffic from the broker extended its deadline", elapsed)
	}
}
