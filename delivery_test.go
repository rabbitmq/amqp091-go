// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"errors"
	"testing"
)

func shouldNotPanic(t *testing.T) {
	if err := recover(); err != nil {
		t.Fatalf("should not panic, got: %s", err)
	}
}

// A closed delivery chan could produce zero value.  Ack/Nack/Reject on these
// deliveries can produce a nil pointer panic. Instead, return an error when
// the method can never be successful.
func TestAckZeroValueAcknowledgerDoesNotPanic(t *testing.T) {
	defer shouldNotPanic(t)
	err := (Delivery{}).Ack(false)
	if err == nil {
		t.Fatalf("expected Delivery{}.Ack to error")
	}
	if !errors.Is(err, ErrDeliveryNotInitialized) {
		t.Fatalf("expected '%v' got '%v'", ErrDeliveryNotInitialized, err)
	}
}

func TestNackZeroValueAcknowledgerDoesNotPanic(t *testing.T) {
	defer shouldNotPanic(t)
	err := (Delivery{}).Nack(false, false)
	if err == nil {
		t.Fatalf("expected Delivery{}.Nack to error")
	}
	if !errors.Is(err, ErrDeliveryNotInitialized) {
		t.Fatalf("expected '%v' got '%v'", ErrDeliveryNotInitialized, err)
	}
}

func TestRejectZeroValueAcknowledgerDoesNotPanic(t *testing.T) {
	defer shouldNotPanic(t)
	err := (Delivery{}).Reject(false)
	if err == nil {
		t.Fatalf("expected Delivery{}.Reject to error")
	}
	if !errors.Is(err, ErrDeliveryNotInitialized) {
		t.Fatalf("expected '%v' got '%v'", ErrDeliveryNotInitialized, err)
	}
}
