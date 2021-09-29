// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"io"
	"testing"
)

type pipe struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (p pipe) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

func (p pipe) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

func (p pipe) Close() error {
	p.r.Close()
	p.w.Close()
	return nil
}

type logIO struct {
	t      *testing.T
	prefix string
	proxy  io.ReadWriteCloser
}

func (log *logIO) Read(p []byte) (n int, err error) {
	return log.proxy.Read(p)
}

func (log *logIO) Write(p []byte) (n int, err error) {
	return log.proxy.Write(p)
}

func (log *logIO) Close() (err error) {
	return log.proxy.Close()
}
