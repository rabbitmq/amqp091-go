// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConfirmOneResequences(t *testing.T) {
	var (
		fixtures = []Confirmation{
			{1, true},
			{2, false},
			{3, true},
		}
		c = newConfirms()
		l = make(chan Confirmation, len(fixtures))
	)

	c.Listen(l)

	for i := range fixtures {
		if want, got := uint64(i+1), c.publish(); want != got.DeliveryTag {
			t.Fatalf("expected publish to return the 1 based delivery tag published, want: %d, got: %d", want, got.DeliveryTag)
		}
	}

	c.One(fixtures[1])
	c.One(fixtures[2])

	select {
	case confirm := <-l:
		t.Fatalf("expected to wait in order to properly resequence results, got: %+v", confirm)
	default:
	}

	c.One(fixtures[0])

	for i, fix := range fixtures {
		if want, got := fix, <-l; want != got {
			t.Fatalf("expected to return confirmations in sequence for %d, want: %+v, got: %+v", i, want, got)
		}
	}
}

func TestConfirmAndPublishDoNotDeadlock(t *testing.T) {
	var (
		c          = newConfirms()
		l          = make(chan Confirmation)
		iterations = 10
	)
	c.Listen(l)

	go func() {
		for i := 0; i < iterations; i++ {
			c.One(Confirmation{uint64(i + 1), true})
		}
	}()

	for i := 0; i < iterations; i++ {
		c.publish()
		<-l
	}
}

func TestConfirmMixedResequences(t *testing.T) {
	var (
		fixtures = []Confirmation{
			{1, true},
			{2, true},
			{3, true},
		}
		c = newConfirms()
		l = make(chan Confirmation, len(fixtures))
	)
	c.Listen(l)

	for range fixtures {
		c.publish()
	}

	c.One(fixtures[0])
	c.One(fixtures[2])
	c.Multiple(fixtures[1])

	for i, fix := range fixtures {
		want := fix
		var got Confirmation
		select {
		case got = <-l:
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout on reading confirmations")
		}
		if want != got {
			t.Fatalf("expected to confirm in sequence for %d, want: %+v, got: %+v", i, want, got)
		}
	}
}

func TestConfirmMultipleResequences(t *testing.T) {
	var (
		fixtures = []Confirmation{
			{1, true},
			{2, true},
			{3, true},
			{4, true},
		}
		c = newConfirms()
		l = make(chan Confirmation, len(fixtures))
	)
	c.Listen(l)

	for range fixtures {
		c.publish()
	}

	c.Multiple(fixtures[len(fixtures)-1])

	for i, fix := range fixtures {
		if want, got := fix, <-l; want != got {
			t.Fatalf("expected to confirm multiple in sequence for %d, want: %+v, got: %+v", i, want, got)
		}
	}
}

func BenchmarkSequentialBufferedConfirms(t *testing.B) {
	var (
		c = newConfirms()
		l = make(chan Confirmation, 10)
	)

	c.Listen(l)

	for i := 0; i < t.N; i++ {
		if i > cap(l)-1 {
			<-l
		}
		c.One(Confirmation{c.publish().DeliveryTag, true})
	}
}

func TestConfirmsIsThreadSafe(t *testing.T) {
	const count = 1000
	const timeout = 5 * time.Second
	var (
		c    = newConfirms()
		l    = make(chan Confirmation)
		pub  = make(chan Confirmation)
		done = make(chan Confirmation)
		late = time.After(timeout)
	)

	c.Listen(l)

	for i := 0; i < count; i++ {
		go func() { pub <- Confirmation{c.publish().DeliveryTag, true} }()
	}

	for i := 0; i < count; i++ {
		go func() { c.One(<-pub) }()
	}

	for i := 0; i < count; i++ {
		go func() { done <- <-l }()
	}

	for i := 0; i < count; i++ {
		select {
		case <-done:
		case <-late:
			t.Fatalf("expected all publish/confirms to finish after %s", timeout)
		}
	}
}

func TestDeferredConfirmationsConfirm(t *testing.T) {
	dcs := newDeferredConfirmations()
	var wg sync.WaitGroup
	for i, ack := range []bool{true, false} {
		var result bool
		deliveryTag := uint64(i + 1)
		dc := dcs.Add(deliveryTag)
		wg.Add(1)
		go func() {
			result = dc.Wait()
			wg.Done()
		}()
		dcs.Confirm(Confirmation{deliveryTag, ack})
		wg.Wait()
		if result != ack {
			t.Fatalf("expected to receive matching ack got %v", result)
		}
	}
}

func TestDeferredConfirmationsConfirmMultiple(t *testing.T) {
	dcs := newDeferredConfirmations()
	var wg sync.WaitGroup
	var result bool
	dc1 := dcs.Add(1)
	dc2 := dcs.Add(2)
	dc3 := dcs.Add(3)
	wg.Add(1)
	go func() {
		result = dc1.Wait() && dc2.Wait() && dc3.Wait()
		wg.Done()
	}()
	dcs.ConfirmMultiple(Confirmation{4, true})
	wg.Wait()
	if !result {
		t.Fatal("expected to receive true for result, received false")
	}
}

func TestDeferredConfirmationsClose(t *testing.T) {
	dcs := newDeferredConfirmations()
	var wg sync.WaitGroup
	var result bool
	dc1 := dcs.Add(1)
	dc2 := dcs.Add(2)
	dc3 := dcs.Add(3)
	wg.Add(1)
	go func() {
		result = !dc1.Wait() && !dc2.Wait() && !dc3.Wait()
		wg.Done()
	}()
	dcs.Close()
	wg.Wait()
	if !result {
		t.Fatal("expected to receive false for nacked confirmations, received true")
	}
}

func TestDeferredConfirmationsDoneAcked(t *testing.T) {
	dcs := newDeferredConfirmations()
	dc := dcs.Add(1)

	if dc.Acked() {
		t.Fatal("expected to receive false for pending confirmations, received true")
	}

	// Confirm twice to ensure that setAck is called once.
	for i := 0; i < 2; i++ {
		dcs.Confirm(Confirmation{dc.DeliveryTag, true})
	}

	<-dc.Done()

	if !dc.Acked() {
		t.Fatal("expected to receive true for acked confirmations, received false")
	}
}

func TestDeferredConfirmationsWaitContextNack(t *testing.T) {
	dcs := newDeferredConfirmations()
	dc := dcs.Add(1)

	dcs.Confirm(Confirmation{dc.DeliveryTag, false})

	ack, err := dc.WaitContext(context.Background())
	if err != nil {
		t.Fatalf("expected to receive nil, got %v", err)
	}
	if ack {
		t.Fatal("expected to receive false for nacked confirmations, received true")
	}
}

func TestDeferredConfirmationsWaitContextCancel(t *testing.T) {
	dc := newDeferredConfirmations().Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ack, err := dc.WaitContext(ctx)
	if err == nil {
		t.Fatal("expected to receive context error, got nil")
	}
	if ack {
		t.Fatal("expected to receive false for pending confirmations, received true")
	}
}

func TestDeferredConfirmationsConcurrency(t *testing.T) {
	dcs := newDeferredConfirmations()
	var wg sync.WaitGroup
	var result bool
	dc1 := dcs.Add(1)
	dc2 := dcs.Add(2)
	dc3 := dcs.Add(3)
	wg.Add(1)

	go func() {
		defer wg.Done()
		result = dc1.Wait() && dc2.Wait() && dc3.Wait()
	}()
	dcs.ConfirmMultiple(Confirmation{4, true})
	wg.Wait()
	if !result {
		t.Fatal("expected to receive true for concurrent confirmations, received false")
	}
}
