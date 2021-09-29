// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import "testing"

func shouldNotPanic(t *testing.T) {
	if err := recover(); err != nil {
		t.Fatalf("should not panic, got: %s", err)
	}
}

// A closed delivery chan could produce zero value.  Ack/Nack/Reject on these
// deliveries can produce a nil pointer panic.  Instead return an error when
// the method can never be successful.
func TestAckZeroValueAcknowledgerDoesNotPanic(t *testing.T) {
	defer shouldNotPanic(t)
	if err := (Delivery{}).Ack(false); err == nil {
		t.Errorf("expected Delivery{}.Ack to error")
	}
}

func TestNackZeroValueAcknowledgerDoesNotPanic(t *testing.T) {
	defer shouldNotPanic(t)
	if err := (Delivery{}).Nack(false, false); err == nil {
		t.Errorf("expected Delivery{}.Ack to error")
	}
}

func TestRejectZeroValueAcknowledgerDoesNotPanic(t *testing.T) {
	defer shouldNotPanic(t)
	if err := (Delivery{}).Reject(false); err == nil {
		t.Errorf("expected Delivery{}.Ack to error")
	}
}
