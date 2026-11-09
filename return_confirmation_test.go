// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDeferredConfirmationReturnIdentifiers rejects returns without a pending tag.
func TestDeferredConfirmationReturnIdentifiers(t *testing.T) {
	for _, id := range []any{nil, []byte{}, make([]byte, 7), make([]byte, 9), make([]byte, 8), binary.BigEndian.AppendUint64(nil, 99), "1", uint64(1)} {
		t.Run(fmt.Sprintf("%T/%v", id, id), func(t *testing.T) {
			d := newDeferredConfirmations()
			dc := d.Add(1)

			d.Return(&Return{Headers: Table{returnIDHeader: id}})
			d.Confirm(Confirmation{1, true})

			if dc.Returned() != nil {
				t.Fatalf("attached return with invalid identifier %v", id)
			}
		})
	}
}

// TestDeferredConfirmationReturnedConcurrentRead exercises completion synchronization.
func TestDeferredConfirmationReturnedConcurrentRead(t *testing.T) {
	d := newDeferredConfirmations()
	dc := d.Add(1)
	ret := &Return{Headers: Table{returnIDHeader: binary.BigEndian.AppendUint64(nil, 1)}, Body: []byte("returned")}

	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()

			for {
				_ = dc.Returned()

				select {
				case <-dc.Done():
					if dc.Returned() != ret {
						t.Error("missing return after completion")
					}

					return
				default:
				}
			}
		}()
	}

	d.Return(ret)
	d.Confirm(Confirmation{1, true})

	readers.Wait()
}

// TestDeferredConfirmationReturnCleanup verifies delivery tag reuse after cleanup.
func TestDeferredConfirmationReturnCleanup(t *testing.T) {
	for _, reset := range []bool{false, true} {
		c := newConfirms()
		old := c.publish()
		c.deferredConfirmations.Return(&Return{Headers: Table{returnIDHeader: binary.BigEndian.AppendUint64(nil, 1)}})

		if reset {
			c.reset()
		} else {
			c.unpublish()
		}

		if old.Wait() || old.Returned() == nil {
			t.Fatal("cleanup did not preserve the returned, unacknowledged outcome")
		}

		next := c.publish()
		c.One(Confirmation{next.DeliveryTag, true})

		if next.DeliveryTag != 1 || !next.Wait() || next.Returned() != nil {
			t.Fatal("reused delivery tag inherited the old return")
		}
	}
}

// TestReturnDispatch correlates out-of-order and multiple confirms with returns.
func TestReturnDispatch(t *testing.T) {
	for _, listen := range []bool{false, true} {
		t.Run(fmt.Sprintf("listener=%v", listen), func(t *testing.T) {
			ch := &Channel{confirms: newConfirms()}
			ch.confirming.Store(true)

			var notifications chan Return
			if listen {
				notifications = ch.NotifyReturn(make(chan Return, 3))
			}

			first := ch.confirms.publish()
			second := ch.confirms.publish()
			third := ch.confirms.publish()

			ch.dispatch(&basicReturn{ReplyCode: NoRoute, Properties: properties{Headers: Table{returnIDHeader: binary.BigEndian.AppendUint64(nil, 3)}}, Body: []byte("third")})
			ch.dispatch(&basicAck{DeliveryTag: 2})
			ch.dispatch(&basicReturn{ReplyCode: NoRoute, Properties: properties{Headers: Table{returnIDHeader: binary.BigEndian.AppendUint64(nil, 1)}}, Body: []byte("first")})
			ch.dispatch(&basicReturn{ReplyCode: NoRoute, Body: []byte("unmatched")})
			ch.dispatch(&basicAck{DeliveryTag: 3, Multiple: true})

			if !first.Wait() || !second.Wait() || !third.Wait() {
				t.Fatal("expected positive acknowledgments")
			}

			firstReturn := first.Returned()
			if firstReturn == nil {
				t.Fatal("expected a return for the first publishing")
			}

			if got := string(firstReturn.Body); got != "first" {
				t.Fatalf("first return body = %q, want %q", got, "first")
			}

			if second.Returned() != nil {
				t.Fatal("expected no return for the second publishing")
			}

			thirdReturn := third.Returned()
			if thirdReturn == nil {
				t.Fatal("expected a return for the third publishing")
			}

			if got := string(thirdReturn.Body); got != "third" {
				t.Fatalf("third return body = %q, want %q", got, "third")
			}

			if listen {
				for _, want := range []string{"third", "first", "unmatched"} {
					select {
					case got := <-notifications:
						if string(got.Body) != want {
							t.Fatalf("notification body = %q, want %q", got.Body, want)
						}
					case <-time.After(time.Second):
						t.Fatal("return notification missing")
					}
				}
			}
		})
	}
}

// TestPublishReturnSendFailure checks cleanup through the publishing entry point.
func TestPublishReturnSendFailure(t *testing.T) {
	ch := &Channel{confirms: newConfirms()}
	ch.confirming.Store(true)
	ch.closed.Store(true)

	headers := Table{returnIDHeader: "previous"}

	dc, err := ch.PublishWithDeferredConfirm("", "route", true, false, Publishing{Headers: headers})
	if err != ErrClosed || dc != nil {
		t.Fatalf("closed publish: confirmation=%v, err=%v", dc, err)
	}

	if ch.confirms.published != 0 || len(ch.confirms.deferredConfirmations.confirmations) != 0 {
		t.Fatal("failed publish retained its delivery tag or confirmation")
	}

	if headers[returnIDHeader] != "previous" {
		t.Fatal("failed publish modified the caller's headers")
	}
}
