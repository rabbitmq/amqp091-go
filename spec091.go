// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/* GENERATED FILE - DO NOT EDIT */
/* Rebuild from the spec/gen.go tool */

package amqp091

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Error codes that can be sent from the server during a connection or
// channel exception or used by the client to indicate a class of error like
// ErrCredentials.  The text of the error is likely more interesting than
// these constants.
const (
	FrameMethod = 1
	FrameHeader    = 2
	FrameBody      = 3
	FrameHeartbeat = 8
	FrameMinSize = 4096
	FrameEnd        = 206
	ReplySuccess    = 200
	ContentTooLarge = 311
	NoRoute            = 312
	NoConsumers        = 313
	ConnectionForced   = 320
	InvalidPath        = 402
	AccessRefused      = 403
	NotFound           = 404
	ResourceLocked     = 405
	PreconditionFailed = 406
	FrameError         = 501
	SyntaxError        = 502
	CommandInvalid     = 503
	ChannelError       = 504
	UnexpectedFrame    = 505
	ResourceError      = 506
	NotAllowed         = 530
	NotImplemented     = 540
	InternalError      = 541
)

// Message classes
const (
	ClassConnection uint16 = 10
	ClassChannel    uint16 = 20
	ClassExchange   uint16 = 40
	ClassQueue      uint16 = 50
	ClassBasic      uint16 = 60
	ClassTx         uint16 = 90
	ClassConfirm    uint16 = 85
)

// Methods
const (
	MethodConnectionStart          uint16 = 10
	MethodConnectionStartOk        uint16 = 11
	MethodConnectionSecure         uint16 = 20
	MethodConnectionSecureOk       uint16 = 21
	MethodConnectionTune           uint16 = 30
	MethodConnectionTuneOk         uint16 = 31
	MethodConnectionOpen           uint16 = 40
	MethodConnectionOpenOk         uint16 = 41
	MethodConnectionClose          uint16 = 50
	MethodConnectionCloseOk        uint16 = 51
	MethodConnectionBlocked        uint16 = 60
	MethodConnectionUnblocked      uint16 = 61
	MethodConnectionUpdateSecret   uint16 = 70
	MethodConnectionUpdateSecretOk uint16 = 71

	MethodChannelOpen    uint16 = 10
	MethodChannelOpenOk  uint16 = 11
	MethodChannelFlow    uint16 = 20
	MethodChannelFlowOk  uint16 = 21
	MethodChannelClose   uint16 = 40
	MethodChannelCloseOk uint16 = 41

	MethodExchangeDeclare   uint16 = 10
	MethodExchangeDeclareOk uint16 = 11
	MethodExchangeDelete    uint16 = 20
	MethodExchangeDeleteOk  uint16 = 21
	MethodExchangeBind      uint16 = 30
	MethodExchangeBindOk    uint16 = 31
	MethodExchangeUnbind    uint16 = 40
	MethodExchangeUnbindOk  uint16 = 51

	MethodQueueDeclare   uint16 = 10
	MethodQueueDeclareOk uint16 = 11
	MethodQueueBind      uint16 = 20
	MethodQueueBindOk    uint16 = 21
	MethodQueueUnbind    uint16 = 50
	MethodQueueUnbindOk  uint16 = 51
	MethodQueuePurge     uint16 = 30
	MethodQueuePurgeOk   uint16 = 31
	MethodQueueDelete    uint16 = 40
	MethodQueueDeleteOk  uint16 = 41

	MethodBasicQos          uint16 = 10
	MethodBasicQosOk        uint16 = 11
	MethodBasicConsume      uint16 = 20
	MethodBasicConsumeOk    uint16 = 21
	MethodBasicCancel       uint16 = 30
	MethodBasicCancelOk     uint16 = 31
	MethodBasicPublish      uint16 = 40
	MethodBasicReturn       uint16 = 50
	MethodBasicDeliver      uint16 = 60
	MethodBasicGet          uint16 = 70
	MethodBasicGetOk        uint16 = 71
	MethodBasicGetEmpty     uint16 = 72
	MethodBasicAck          uint16 = 80
	MethodBasicReject       uint16 = 90
	MethodBasicRecoverAsync uint16 = 100
	MethodBasicRecover      uint16 = 110
	MethodBasicRecoverOk    uint16 = 111
	MethodBasicNack         uint16 = 120

	MethodTxSelect     uint16 = 10
	MethodTxSelectOk   uint16 = 11
	MethodTxCommit     uint16 = 20
	MethodTxCommitOk   uint16 = 21
	MethodTxRollback   uint16 = 30
	MethodTxRollbackOk uint16 = 31

	MethodConfirmSelect   uint16 = 10
	MethodConfirmSelectOk uint16 = 11
)

func GetMethodString(class, method uint16) string {
	classes := map[uint16]string{
		10: "Connection",
		20: "Channel",
		40: "Exchange",
		50: "Queue",
		60: "Basic",
		90: "Tx",
		85: "Confirm",
	}
	methods := map[uint16]map[uint16]string{
		10: { // Connection
			10: "Start",
			11: "Start-Ok",
			20: "Secure",
			21: "Secure-Ok",
			30: "Tune",
			31: "Tune-Ok",
			40: "Open",
			41: "Open-Ok",
			50: "Close",
			51: "Close-Ok",
			60: "Blocked",
			61: "Unblocked",
			70: "Update-Secret",
			71: "Update-Secret-Ok",
		},
		20: { // Channel
			10: "Open",
			11: "Open-Ok",
			20: "Flow",
			21: "Flow-Ok",
			40: "Close",
			41: "Close-Ok",
		},
		40: { // Exchange
			10: "Declare",
			11: "Declare-Ok",
			20: "Delete",
			21: "Delete-Ok",
			30: "Bind",
			31: "Bind-Ok",
			40: "Unbind",
			51: "Unbind-Ok",
		},
		50: { // Queue
			10: "Declare",
			11: "Declare-Ok",
			20: "Bind",
			21: "Bind-Ok",
			50: "Unbind",
			51: "Unbind-Ok",
			30: "Purge",
			31: "Purge-Ok",
			40: "Delete",
			41: "Delete-Ok",
		},
		60: { // Basic
			10:  "Qos",
			11:  "Qos-Ok",
			20:  "Consume",
			21:  "Consume-Ok",
			30:  "Cancel",
			31:  "Cancel-Ok",
			40:  "Publish",
			50:  "Return",
			60:  "Deliver",
			70:  "Get",
			71:  "Get-Ok",
			72:  "Get-Empty",
			80:  "Ack",
			90:  "Reject",
			100: "Recover-Async",
			110: "Recover",
			111: "Recover-Ok",
			120: "Nack",
		},
		90: { // Tx
			10: "Select",
			11: "Select-Ok",
			20: "Commit",
			21: "Commit-Ok",
			30: "Rollback",
			31: "Rollback-Ok",
		},
		85: { // Confirm
			10: "Select",
			11: "Select-Ok",
		},
	}

	className, exist := classes[class]
	if !exist {
		return "invalid"
	}
	methodName, exist := methods[class][method]
	if !exist {
		return "invalid"
	}

	return fmt.Sprintf("%s.%s", className, methodName)
}

func IsSoftExceptionCode(code int) bool {
	switch code {
	case 311:
		return true
	case 312:
		return true
	case 313:
		return true
	case 403:
		return true
	case 404:
		return true
	case 405:
		return true
	case 406:
		return true

	}
	return false
}

type ConnectionStart struct {
	VersionMajor     byte
	VersionMinor     byte
	ServerProperties Table
	Mechanisms       string
	Locales          string
}

func (msg *ConnectionStart) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionStart
}

func (msg *ConnectionStart) Wait() bool {
	return true
}

func (msg *ConnectionStart) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.VersionMajor); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.VersionMinor); err != nil {
		return err
	}
	if err = writeTable(w, msg.ServerProperties); err != nil {
		return err
	}
	if err = writeLongstr(w, msg.Mechanisms); err != nil {
		return err
	}
	if err = writeLongstr(w, msg.Locales); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionStart) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.VersionMajor); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.VersionMinor); err != nil {
		return err
	}
	if msg.ServerProperties, err = readTable(r); err != nil {
		return err
	}
	if msg.Mechanisms, err = readLongstr(r); err != nil {
		return err
	}
	if msg.Locales, err = readLongstr(r); err != nil {
		return err
	}

	return nil
}

type ConnectionStartOk struct {
	ClientProperties Table
	Mechanism        string
	Response         string
	Locale           string
}

func (msg *ConnectionStartOk) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionStartOk
}

func (msg *ConnectionStartOk) Wait() bool {
	return true
}

func (msg *ConnectionStartOk) Write(w io.Writer) (err error) {
	if err = writeTable(w, msg.ClientProperties); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Mechanism); err != nil {
		return err
	}
	if err = writeLongstr(w, msg.Response); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Locale); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionStartOk) Read(r io.Reader) (err error) {
	if msg.ClientProperties, err = readTable(r); err != nil {
		return err
	}
	if msg.Mechanism, err = readShortstr(r); err != nil {
		return err
	}
	if msg.Response, err = readLongstr(r); err != nil {
		return err
	}
	if msg.Locale, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type ConnectionSecure struct {
	Challenge string
}

func (msg *ConnectionSecure) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionSecure
}

func (msg *ConnectionSecure) Wait() bool {
	return true
}

func (msg *ConnectionSecure) Write(w io.Writer) (err error) {
	if err = writeLongstr(w, msg.Challenge); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionSecure) Read(r io.Reader) (err error) {
	if msg.Challenge, err = readLongstr(r); err != nil {
		return err
	}

	return nil
}

type ConnectionSecureOk struct {
	Response string
}

func (msg *ConnectionSecureOk) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionSecureOk
}

func (msg *ConnectionSecureOk) Wait() bool {
	return true
}

func (msg *ConnectionSecureOk) Write(w io.Writer) (err error) {
	if err = writeLongstr(w, msg.Response); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionSecureOk) Read(r io.Reader) (err error) {
	if msg.Response, err = readLongstr(r); err != nil {
		return err
	}

	return nil
}

type ConnectionTune struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (msg *ConnectionTune) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionTune
}

func (msg *ConnectionTune) Wait() bool {
	return true
}

func (msg *ConnectionTune) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.ChannelMax); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.FrameMax); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.Heartbeat); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionTune) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.ChannelMax); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.FrameMax); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.Heartbeat); err != nil {
		return err
	}

	return nil
}

type ConnectionTuneOk struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (msg *ConnectionTuneOk) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionTuneOk
}

func (msg *ConnectionTuneOk) Wait() bool {
	return true
}

func (msg *ConnectionTuneOk) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.ChannelMax); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.FrameMax); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.Heartbeat); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionTuneOk) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.ChannelMax); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.FrameMax); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.Heartbeat); err != nil {
		return err
	}

	return nil
}

type ConnectionOpen struct {
	VirtualHost string
	reserved1   string
	reserved2   bool
}

func (msg *ConnectionOpen) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionOpen
}

func (msg *ConnectionOpen) Wait() bool {
	return true
}

func (msg *ConnectionOpen) Write(w io.Writer) (err error) {
	var bits byte

	if err = writeShortstr(w, msg.VirtualHost); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.reserved1); err != nil {
		return err
	}
	if msg.reserved2 {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionOpen) Read(r io.Reader) (err error) {
	var bits byte

	if msg.VirtualHost, err = readShortstr(r); err != nil {
		return err
	}
	if msg.reserved1, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.reserved2 = bits&(1<<0) > 0

	return nil
}

type ConnectionOpenOk struct {
	reserved1 string
}

func (msg *ConnectionOpenOk) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionOpenOk
}

func (msg *ConnectionOpenOk) Wait() bool {
	return true
}

func (msg *ConnectionOpenOk) Write(w io.Writer) (err error) {
	if err = writeShortstr(w, msg.reserved1); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionOpenOk) Read(r io.Reader) (err error) {
	if msg.reserved1, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type ConnectionClose struct {
	ReplyCode uint16
	ReplyText string
	ClassId   uint16
	MethodId  uint16
}

func (msg *ConnectionClose) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionClose
}

func (msg *ConnectionClose) Wait() bool {
	return true
}

func (msg *ConnectionClose) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.ReplyCode); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.ReplyText); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.ClassId); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.MethodId); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionClose) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.ReplyCode); err != nil {
		return err
	}
	if msg.ReplyText, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.ClassId); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.MethodId); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionClose) GetReplyCode() uint16 {
	return msg.ReplyCode
}

func (msg *ConnectionClose) GetReplyText() string {
	return msg.ReplyText
}

func (msg *ConnectionClose) GetClassId() uint16 {
	return msg.ClassId
}

func (msg *ConnectionClose) GetMethodId() uint16 {
	return msg.MethodId
}

type ConnectionCloseOk struct {
}

func (msg *ConnectionCloseOk) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionCloseOk
}

func (msg *ConnectionCloseOk) Wait() bool {
	return true
}

func (msg *ConnectionCloseOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ConnectionCloseOk) Read(_ io.Reader) (err error) {
	return nil
}

type ConnectionBlocked struct {
	Reason string
}

func (msg *ConnectionBlocked) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionBlocked
}

func (msg *ConnectionBlocked) Wait() bool {
	return false
}

func (msg *ConnectionBlocked) Write(w io.Writer) (err error) {
	if err = writeShortstr(w, msg.Reason); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionBlocked) Read(r io.Reader) (err error) {
	if msg.Reason, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type ConnectionUnblocked struct {
}

func (msg *ConnectionUnblocked) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionUnblocked
}

func (msg *ConnectionUnblocked) Wait() bool {
	return false
}

func (msg *ConnectionUnblocked) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ConnectionUnblocked) Read(_ io.Reader) (err error) {
	return nil
}

// UpdateSecret and UpdateSecretOk spec can be found here
// https://github.com/rabbitmq/rabbitmq-dotnet-client/pull/674/files

type ConnectionUpdateSecret struct {
	NewSecret string
	Reason    string
}

func (msg *ConnectionUpdateSecret) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionUpdateSecret
}

func (msg *ConnectionUpdateSecret) Wait() bool {
	return true
}

func (msg *ConnectionUpdateSecret) Write(w io.Writer) (err error) {
	if err = writeLongstr(w, msg.NewSecret); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Reason); err != nil {
		return err
	}

	return nil
}

func (msg *ConnectionUpdateSecret) Read(r io.Reader) (err error) {
	if msg.NewSecret, err = readLongstr(r); err != nil {
		return err
	}
	if msg.Reason, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type ConnectionUpdateSecretOk struct {
}

func (msg *ConnectionUpdateSecretOk) Id() (uint16, uint16) {
	return ClassConnection, MethodConnectionUpdateSecretOk
}

func (msg *ConnectionUpdateSecretOk) Wait() bool {
	return false
}

func (msg *ConnectionUpdateSecretOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ConnectionUpdateSecretOk) Read(_ io.Reader) (err error) {
	return nil
}

type ChannelOpen struct {
	reserved1 string
}

func (msg *ChannelOpen) Id() (uint16, uint16) {
	return ClassChannel, MethodChannelOpen
}

func (msg *ChannelOpen) Wait() bool {
	return true
}

func (msg *ChannelOpen) Write(w io.Writer) (err error) {
	if err = writeShortstr(w, msg.reserved1); err != nil {
		return err
	}

	return nil
}

func (msg *ChannelOpen) Read(r io.Reader) (err error) {
	if msg.reserved1, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type ChannelOpenOk struct {
	reserved1 string
}

func (msg *ChannelOpenOk) Id() (uint16, uint16) {
	return ClassChannel, MethodChannelOpenOk
}

func (msg *ChannelOpenOk) Wait() bool {
	return true
}

func (msg *ChannelOpenOk) Write(w io.Writer) (err error) {
	if err = writeLongstr(w, msg.reserved1); err != nil {
		return err
	}

	return nil
}

func (msg *ChannelOpenOk) Read(r io.Reader) (err error) {
	if msg.reserved1, err = readLongstr(r); err != nil {
		return err
	}

	return nil
}

type ChannelFlow struct {
	Active bool
}

func (msg *ChannelFlow) Id() (uint16, uint16) {
	return ClassChannel, MethodChannelFlow
}

func (msg *ChannelFlow) Wait() bool {
	return true
}

func (msg *ChannelFlow) Write(w io.Writer) (err error) {
	var bits byte

	if msg.Active {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *ChannelFlow) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Active = bits&(1<<0) > 0

	return nil
}

type ChannelFlowOk struct {
	Active bool
}

func (msg *ChannelFlowOk) Id() (uint16, uint16) {
	return ClassChannel, MethodChannelFlowOk
}

func (msg *ChannelFlowOk) Wait() bool {
	return true
}

func (msg *ChannelFlowOk) Write(w io.Writer) (err error) {
	var bits byte

	if msg.Active {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *ChannelFlowOk) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Active = bits&(1<<0) > 0

	return nil
}

type ChannelClose struct {
	ReplyCode uint16
	ReplyText string
	ClassId   uint16
	MethodId  uint16
}

func (msg *ChannelClose) Id() (uint16, uint16) {
	return ClassChannel, MethodChannelClose
}

func (msg *ChannelClose) Wait() bool {
	return true
}

func (msg *ChannelClose) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.ReplyCode); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.ReplyText); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.ClassId); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.MethodId); err != nil {
		return err
	}

	return nil
}

func (msg *ChannelClose) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.ReplyCode); err != nil {
		return err
	}
	if msg.ReplyText, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.ClassId); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.MethodId); err != nil {
		return err
	}

	return nil
}

func (msg *ChannelClose) GetReplyCode() uint16 {
	return msg.ReplyCode
}

func (msg *ChannelClose) GetReplyText() string {
	return msg.ReplyText
}

func (msg *ChannelClose) GetClassId() uint16 {
	return msg.ClassId
}

func (msg *ChannelClose) GetMethodId() uint16 {
	return msg.MethodId
}

type ChannelCloseOk struct {
}

func (msg *ChannelCloseOk) Id() (uint16, uint16) {
	return ClassChannel, MethodChannelCloseOk
}

func (msg *ChannelCloseOk) Wait() bool {
	return true
}

func (msg *ChannelCloseOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ChannelCloseOk) Read(_ io.Reader) (err error) {
	return nil
}

type ExchangeDeclare struct {
	reserved1  uint16
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  Table
}

func (msg *ExchangeDeclare) Id() (uint16, uint16) {
	return ClassExchange, MethodExchangeDeclare
}

func (msg *ExchangeDeclare) Wait() bool {
	return !msg.NoWait
}

func (msg *ExchangeDeclare) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Type); err != nil {
		return err
	}
	if msg.Passive {
		bits |= 1 << 0
	}
	if msg.Durable {
		bits |= 1 << 1
	}
	if msg.AutoDelete {
		bits |= 1 << 2
	}
	if msg.Internal {
		bits |= 1 << 3
	}
	if msg.NoWait {
		bits |= 1 << 4
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}
	if err = writeTable(w, msg.Arguments); err != nil {
		return err
	}

	return nil
}

func (msg *ExchangeDeclare) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Exchange, err = readShortstr(r); err != nil {
		return err
	}
	if msg.Type, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Passive = bits&(1<<0) > 0
	msg.Durable = bits&(1<<1) > 0
	msg.AutoDelete = bits&(1<<2) > 0
	msg.Internal = bits&(1<<3) > 0
	msg.NoWait = bits&(1<<4) > 0

	if msg.Arguments, err = readTable(r); err != nil {
		return err
	}

	return nil
}

type ExchangeDeclareOk struct {
}

func (msg *ExchangeDeclareOk) Id() (uint16, uint16) {
	return ClassExchange, MethodExchangeDeclareOk
}

func (msg *ExchangeDeclareOk) Wait() bool {
	return true
}

func (msg *ExchangeDeclareOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ExchangeDeclareOk) Read(_ io.Reader) (err error) {
	return nil
}

type ExchangeDelete struct {
	reserved1 uint16
	Exchange  string
	IfUnused  bool
	NoWait    bool
}

func (msg *ExchangeDelete) Id() (uint16, uint16) {
	return ClassExchange, MethodExchangeDelete
}

func (msg *ExchangeDelete) Wait() bool {
	return !msg.NoWait
}

func (msg *ExchangeDelete) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return err
	}
	if msg.IfUnused {
		bits |= 1 << 0
	}
	if msg.NoWait {
		bits |= 1 << 1
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *ExchangeDelete) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Exchange, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.IfUnused = bits&(1<<0) > 0
	msg.NoWait = bits&(1<<1) > 0

	return nil
}

type ExchangeDeleteOk struct {
}

func (msg *ExchangeDeleteOk) Id() (uint16, uint16) {
	return ClassExchange, MethodExchangeDeleteOk
}

func (msg *ExchangeDeleteOk) Wait() bool {
	return true
}

func (msg *ExchangeDeleteOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ExchangeDeleteOk) Read(_ io.Reader) (err error) {
	return nil
}

type ExchangeBind struct {
	reserved1   uint16
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   Table
}

func (msg *ExchangeBind) Id() (uint16, uint16) {
	return ClassExchange, MethodExchangeBind
}

func (msg *ExchangeBind) Wait() bool {
	return !msg.NoWait
}

func (msg *ExchangeBind) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Destination); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Source); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return err
	}
	if msg.NoWait {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}
	if err = writeTable(w, msg.Arguments); err != nil {
		return err
	}

	return nil
}

func (msg *ExchangeBind) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Destination, err = readShortstr(r); err != nil {
		return err
	}
	if msg.Source, err = readShortstr(r); err != nil {
		return err
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.NoWait = bits&(1<<0) > 0

	if msg.Arguments, err = readTable(r); err != nil {
		return err
	}

	return nil
}

type ExchangeBindOk struct {
}

func (msg *ExchangeBindOk) Id() (uint16, uint16) {
	return ClassExchange, MethodExchangeBindOk
}

func (msg *ExchangeBindOk) Wait() bool {
	return true
}

func (msg *ExchangeBindOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ExchangeBindOk) Read(_ io.Reader) (err error) {
	return nil
}

type ExchangeUnbind struct {
	reserved1   uint16
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   Table
}

func (msg *ExchangeUnbind) Id() (uint16, uint16) {
	return ClassExchange, MethodExchangeUnbind
}

func (msg *ExchangeUnbind) Wait() bool {
	return !msg.NoWait
}

func (msg *ExchangeUnbind) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Destination); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Source); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return err
	}
	if msg.NoWait {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}
	if err = writeTable(w, msg.Arguments); err != nil {
		return err
	}

	return nil
}

func (msg *ExchangeUnbind) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Destination, err = readShortstr(r); err != nil {
		return err
	}
	if msg.Source, err = readShortstr(r); err != nil {
		return err
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.NoWait = bits&(1<<0) > 0

	if msg.Arguments, err = readTable(r); err != nil {
		return err
	}

	return nil
}

type ExchangeUnbindOk struct {
}

func (msg *ExchangeUnbindOk) Id() (uint16, uint16) {
	return ClassExchange, MethodExchangeUnbindOk
}

func (msg *ExchangeUnbindOk) Wait() bool {
	return true
}

func (msg *ExchangeUnbindOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ExchangeUnbindOk) Read(_ io.Reader) (err error) {
	return nil
}

type QueueDeclare struct {
	reserved1  uint16
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  Table
}

func (msg *QueueDeclare) Id() (uint16, uint16) {
	return ClassQueue, MethodQueueDeclare
}

func (msg *QueueDeclare) Wait() bool {
	return !msg.NoWait
}

func (msg *QueueDeclare) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Queue); err != nil {
		return err
	}
	if msg.Passive {
		bits |= 1 << 0
	}
	if msg.Durable {
		bits |= 1 << 1
	}
	if msg.Exclusive {
		bits |= 1 << 2
	}
	if msg.AutoDelete {
		bits |= 1 << 3
	}
	if msg.NoWait {
		bits |= 1 << 4
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}
	if err = writeTable(w, msg.Arguments); err != nil {
		return err
	}

	return nil
}

func (msg *QueueDeclare) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Queue, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Passive = bits&(1<<0) > 0
	msg.Durable = bits&(1<<1) > 0
	msg.Exclusive = bits&(1<<2) > 0
	msg.AutoDelete = bits&(1<<3) > 0
	msg.NoWait = bits&(1<<4) > 0

	if msg.Arguments, err = readTable(r); err != nil {
		return err
	}

	return nil
}

type QueueDeclareOk struct {
	Queue         string
	MessageCount  uint32
	ConsumerCount uint32
}

func (msg *QueueDeclareOk) Id() (uint16, uint16) {
	return ClassQueue, MethodQueueDeclareOk
}

func (msg *QueueDeclareOk) Wait() bool {
	return true
}

func (msg *QueueDeclareOk) Write(w io.Writer) (err error) {
	if err = writeShortstr(w, msg.Queue); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.MessageCount); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.ConsumerCount); err != nil {
		return err
	}

	return nil
}

func (msg *QueueDeclareOk) Read(r io.Reader) (err error) {
	if msg.Queue, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.MessageCount); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.ConsumerCount); err != nil {
		return err
	}

	return nil
}

type QueueBind struct {
	reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  Table
}

func (msg *QueueBind) Id() (uint16, uint16) {
	return ClassQueue, MethodQueueBind
}

func (msg *QueueBind) Wait() bool {
	return !msg.NoWait
}

func (msg *QueueBind) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Queue); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return err
	}
	if msg.NoWait {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}
	if err = writeTable(w, msg.Arguments); err != nil {
		return err
	}

	return nil
}

func (msg *QueueBind) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Queue, err = readShortstr(r); err != nil {
		return err
	}
	if msg.Exchange, err = readShortstr(r); err != nil {
		return err
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.NoWait = bits&(1<<0) > 0

	if msg.Arguments, err = readTable(r); err != nil {
		return err
	}

	return nil
}

type QueueBindOk struct {
}

func (msg *QueueBindOk) Id() (uint16, uint16) {
	return ClassQueue, MethodQueueBindOk
}

func (msg *QueueBindOk) Wait() bool {
	return true
}

func (msg *QueueBindOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *QueueBindOk) Read(_ io.Reader) (err error) {
	return nil
}

type QueueUnbind struct {
	reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	Arguments  Table
}

func (msg *QueueUnbind) Id() (uint16, uint16) {
	return ClassQueue, MethodQueueUnbind
}

func (msg *QueueUnbind) Wait() bool {
	return true
}

func (msg *QueueUnbind) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Queue); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return err
	}
	if err = writeTable(w, msg.Arguments); err != nil {
		return err
	}

	return nil
}

func (msg *QueueUnbind) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Queue, err = readShortstr(r); err != nil {
		return err
	}
	if msg.Exchange, err = readShortstr(r); err != nil {
		return err
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return err
	}
	if msg.Arguments, err = readTable(r); err != nil {
		return err
	}

	return nil
}

type QueueUnbindOk struct {
}

func (msg *QueueUnbindOk) Id() (uint16, uint16) {
	return ClassQueue, MethodQueueUnbindOk
}

func (msg *QueueUnbindOk) Wait() bool {
	return true
}

func (msg *QueueUnbindOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *QueueUnbindOk) Read(_ io.Reader) (err error) {
	return nil
}

type QueuePurge struct {
	reserved1 uint16
	Queue     string
	NoWait    bool
}

func (msg *QueuePurge) Id() (uint16, uint16) {
	return ClassQueue, MethodQueuePurge
}

func (msg *QueuePurge) Wait() bool {
	return !msg.NoWait
}

func (msg *QueuePurge) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Queue); err != nil {
		return err
	}
	if msg.NoWait {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *QueuePurge) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Queue, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.NoWait = bits&(1<<0) > 0

	return nil
}

type QueuePurgeOk struct {
	MessageCount uint32
}

func (msg *QueuePurgeOk) Id() (uint16, uint16) {
	return ClassQueue, MethodQueuePurgeOk
}

func (msg *QueuePurgeOk) Wait() bool {
	return true
}

func (msg *QueuePurgeOk) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.MessageCount); err != nil {
		return err
	}

	return nil
}

func (msg *QueuePurgeOk) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.MessageCount); err != nil {
		return err
	}

	return nil
}

type QueueDelete struct {
	reserved1 uint16
	Queue     string
	IfUnused  bool
	IfEmpty   bool
	NoWait    bool
}

func (msg *QueueDelete) Id() (uint16, uint16) {
	return ClassQueue, MethodQueueDelete
}

func (msg *QueueDelete) Wait() bool {
	return !msg.NoWait
}

func (msg *QueueDelete) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Queue); err != nil {
		return err
	}
	if msg.IfUnused {
		bits |= 1 << 0
	}
	if msg.IfEmpty {
		bits |= 1 << 1
	}
	if msg.NoWait {
		bits |= 1 << 2
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *QueueDelete) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Queue, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.IfUnused = bits&(1<<0) > 0
	msg.IfEmpty = bits&(1<<1) > 0
	msg.NoWait = bits&(1<<2) > 0

	return nil
}

type QueueDeleteOk struct {
	MessageCount uint32
}

func (msg *QueueDeleteOk) Id() (uint16, uint16) {
	return ClassQueue, MethodQueueDeleteOk
}

func (msg *QueueDeleteOk) Wait() bool {
	return true
}

func (msg *QueueDeleteOk) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.MessageCount); err != nil {
		return err
	}

	return nil
}

func (msg *QueueDeleteOk) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.MessageCount); err != nil {
		return err
	}

	return nil
}

type BasicQos struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

func (msg *BasicQos) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicQos
}

func (msg *BasicQos) Wait() bool {
	return true
}

func (msg *BasicQos) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.PrefetchSize); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.PrefetchCount); err != nil {
		return err
	}
	if msg.Global {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicQos) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.PrefetchSize); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.PrefetchCount); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Global = bits&(1<<0) > 0

	return nil
}

type BasicQosOk struct {
}

func (msg *BasicQosOk) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicQosOk
}

func (msg *BasicQosOk) Wait() bool {
	return true
}

func (msg *BasicQosOk) Write(_ io.Writer) (err error) {
	return
}

func (msg *BasicQosOk) Read(_ io.Reader) (err error) {
	return
}

type BasicConsume struct {
	reserved1   uint16
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	Arguments   Table
}

func (msg *BasicConsume) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicConsume
}

func (msg *BasicConsume) Wait() bool {
	return !msg.NoWait
}

func (msg *BasicConsume) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Queue); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return err
	}
	if msg.NoLocal {
		bits |= 1 << 0
	}
	if msg.NoAck {
		bits |= 1 << 1
	}
	if msg.Exclusive {
		bits |= 1 << 2
	}
	if msg.NoWait {
		bits |= 1 << 3
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}
	if err = writeTable(w, msg.Arguments); err != nil {
		return err
	}

	return nil
}

func (msg *BasicConsume) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Queue, err = readShortstr(r); err != nil {
		return err
	}
	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.NoLocal = bits&(1<<0) > 0
	msg.NoAck = bits&(1<<1) > 0
	msg.Exclusive = bits&(1<<2) > 0
	msg.NoWait = bits&(1<<3) > 0

	if msg.Arguments, err = readTable(r); err != nil {
		return err
	}

	return nil
}

type BasicConsumeOk struct {
	ConsumerTag string
}

func (msg *BasicConsumeOk) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicConsumeOk
}

func (msg *BasicConsumeOk) Wait() bool {
	return true
}

func (msg *BasicConsumeOk) Write(w io.Writer) (err error) {
	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return err
	}

	return nil
}

func (msg *BasicConsumeOk) Read(r io.Reader) (err error) {
	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type BasicCancel struct {
	ConsumerTag string
	NoWait      bool
}

func (msg *BasicCancel) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicCancel
}

func (msg *BasicCancel) Wait() bool {
	return !msg.NoWait
}

func (msg *BasicCancel) Write(w io.Writer) (err error) {
	var bits byte

	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return err
	}
	if msg.NoWait {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicCancel) Read(r io.Reader) (err error) {
	var bits byte

	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.NoWait = bits&(1<<0) > 0

	return nil
}

type BasicCancelOk struct {
	ConsumerTag string
}

func (msg *BasicCancelOk) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicCancelOk
}

func (msg *BasicCancelOk) Wait() bool {
	return true
}

func (msg *BasicCancelOk) Write(w io.Writer) (err error) {
	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return err
	}

	return nil
}

func (msg *BasicCancelOk) Read(r io.Reader) (err error) {
	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type BasicPublish struct {
	reserved1  uint16
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	Properties Properties
	Body       []byte
}

func (msg *BasicPublish) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicPublish
}

func (msg *BasicPublish) Wait() bool {
	return false
}

func (msg *BasicPublish) GetContent() (Properties, []byte) {
	return msg.Properties, msg.Body
}

func (msg *BasicPublish) SetContent(props Properties, body []byte) {
	msg.Properties, msg.Body = props, body
}

func (msg *BasicPublish) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return err
	}
	if msg.Mandatory {
		bits |= 1 << 0
	}
	if msg.Immediate {
		bits |= 1 << 1
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicPublish) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Exchange, err = readShortstr(r); err != nil {
		return err
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Mandatory = bits&(1<<0) > 0
	msg.Immediate = bits&(1<<1) > 0

	return nil
}

type BasicReturn struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
	Properties Properties
	Body       []byte
}

func (msg *BasicReturn) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicReturn
}

func (msg *BasicReturn) Wait() bool {
	return false
}

func (msg *BasicReturn) GetContent() (Properties, []byte) {
	return msg.Properties, msg.Body
}

func (msg *BasicReturn) SetContent(props Properties, body []byte) {
	msg.Properties, msg.Body = props, body
}

func (msg *BasicReturn) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, msg.ReplyCode); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.ReplyText); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return err
	}

	return nil
}

func (msg *BasicReturn) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &msg.ReplyCode); err != nil {
		return err
	}
	if msg.ReplyText, err = readShortstr(r); err != nil {
		return err
	}
	if msg.Exchange, err = readShortstr(r); err != nil {
		return err
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type BasicDeliver struct {
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string
	Properties  Properties
	Body        []byte
}

func (msg *BasicDeliver) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicDeliver
}

func (msg *BasicDeliver) Wait() bool {
	return false
}

func (msg *BasicDeliver) GetContent() (Properties, []byte) {
	return msg.Properties, msg.Body
}

func (msg *BasicDeliver) SetContent(props Properties, body []byte) {
	msg.Properties, msg.Body = props, body
}

func (msg *BasicDeliver) Write(w io.Writer) (err error) {
	var bits byte

	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return err
	}
	if msg.Redelivered {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return err
	}

	return nil
}

func (msg *BasicDeliver) Read(r io.Reader) (err error) {
	var bits byte

	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Redelivered = bits&(1<<0) > 0

	if msg.Exchange, err = readShortstr(r); err != nil {
		return err
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type BasicGet struct {
	reserved1 uint16
	Queue     string
	NoAck     bool
}

func (msg *BasicGet) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicGet
}

func (msg *BasicGet) Wait() bool {
	return true
}

func (msg *BasicGet) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Queue); err != nil {
		return err
	}
	if msg.NoAck {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicGet) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return err
	}
	if msg.Queue, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.NoAck = bits&(1<<0) > 0

	return nil
}

type BasicGetOk struct {
	DeliveryTag  uint64
	Redelivered  bool
	Exchange     string
	RoutingKey   string
	MessageCount uint32
	Properties   Properties
	Body         []byte
}

func (msg *BasicGetOk) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicGetOk
}

func (msg *BasicGetOk) Wait() bool {
	return true
}

func (msg *BasicGetOk) GetContent() (Properties, []byte) {
	return msg.Properties, msg.Body
}

func (msg *BasicGetOk) SetContent(props Properties, body []byte) {
	msg.Properties, msg.Body = props, body
}

func (msg *BasicGetOk) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return err
	}
	if msg.Redelivered {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return err
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, msg.MessageCount); err != nil {
		return err
	}

	return nil
}

func (msg *BasicGetOk) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Redelivered = bits&(1<<0) > 0

	if msg.Exchange, err = readShortstr(r); err != nil {
		return err
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &msg.MessageCount); err != nil {
		return err
	}

	return nil
}

type BasicGetEmpty struct {
	reserved1 string
}

func (msg *BasicGetEmpty) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicGetEmpty
}

func (msg *BasicGetEmpty) Wait() bool {
	return true
}

func (msg *BasicGetEmpty) Write(w io.Writer) (err error) {
	if err = writeShortstr(w, msg.reserved1); err != nil {
		return err
	}

	return nil
}

func (msg *BasicGetEmpty) Read(r io.Reader) (err error) {
	if msg.reserved1, err = readShortstr(r); err != nil {
		return err
	}

	return nil
}

type BasicAck struct {
	DeliveryTag uint64
	Multiple    bool
}

func (msg *BasicAck) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicAck
}

func (msg *BasicAck) Wait() bool {
	return false
}

func (msg *BasicAck) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return err
	}
	if msg.Multiple {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicAck) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Multiple = bits&(1<<0) > 0

	return nil
}

type BasicReject struct {
	DeliveryTag uint64
	Requeue     bool
}

func (msg *BasicReject) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicReject
}

func (msg *BasicReject) Wait() bool {
	return false
}

func (msg *BasicReject) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return err
	}
	if msg.Requeue {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicReject) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Requeue = bits&(1<<0) > 0

	return nil
}

type BasicRecoverAsync struct {
	Requeue bool
}

func (msg *BasicRecoverAsync) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicRecoverAsync
}

func (msg *BasicRecoverAsync) Wait() bool {
	return false
}

func (msg *BasicRecoverAsync) Write(w io.Writer) (err error) {
	var bits byte

	if msg.Requeue {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicRecoverAsync) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Requeue = bits&(1<<0) > 0

	return nil
}

type BasicRecover struct {
	Requeue bool
}

func (msg *BasicRecover) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicRecover
}

func (msg *BasicRecover) Wait() bool {
	return true
}

func (msg *BasicRecover) Write(w io.Writer) (err error) {
	var bits byte

	if msg.Requeue {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicRecover) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Requeue = bits&(1<<0) > 0

	return nil
}

type BasicRecoverOk struct {
}

func (msg *BasicRecoverOk) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicRecoverOk
}

func (msg *BasicRecoverOk) Wait() bool {
	return true
}

func (msg *BasicRecoverOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *BasicRecoverOk) Read(_ io.Reader) (err error) {
	return nil
}

type BasicNack struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

func (msg *BasicNack) Id() (uint16, uint16) {
	return ClassBasic, MethodBasicNack
}

func (msg *BasicNack) Wait() bool {
	return false
}

func (msg *BasicNack) Write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return err
	}
	if msg.Multiple {
		bits |= 1 << 0
	}
	if msg.Requeue {
		bits |= 1 << 1
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *BasicNack) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return err
	}
	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Multiple = bits&(1<<0) > 0
	msg.Requeue = bits&(1<<1) > 0

	return nil
}

type TxSelect struct {
}

func (msg *TxSelect) Id() (uint16, uint16) {
	return ClassTx, MethodTxSelect
}

func (msg *TxSelect) Wait() bool {
	return true
}

func (msg *TxSelect) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *TxSelect) Read(_ io.Reader) (err error) {
	return nil
}

type TxSelectOk struct {
}

func (msg *TxSelectOk) Id() (uint16, uint16) {
	return ClassTx, MethodTxSelectOk
}

func (msg *TxSelectOk) Wait() bool {
	return true
}

func (msg *TxSelectOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *TxSelectOk) Read(_ io.Reader) (err error) {
	return nil
}

type TxCommit struct {
}

func (msg *TxCommit) Id() (uint16, uint16) {
	return ClassTx, MethodTxCommit
}

func (msg *TxCommit) Wait() bool {
	return true
}

func (msg *TxCommit) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *TxCommit) Read(_ io.Reader) (err error) {
	return nil
}

type TxCommitOk struct {
}

func (msg *TxCommitOk) Id() (uint16, uint16) {
	return ClassTx, MethodTxCommitOk
}

func (msg *TxCommitOk) Wait() bool {
	return true
}

func (msg *TxCommitOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *TxCommitOk) Read(_ io.Reader) (err error) {
	return nil
}

type TxRollback struct {
}

func (msg *TxRollback) Id() (uint16, uint16) {
	return ClassTx, MethodTxRollback
}

func (msg *TxRollback) Wait() bool {
	return true
}

func (msg *TxRollback) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *TxRollback) Read(_ io.Reader) (err error) {
	return nil
}

type TxRollbackOk struct {
}

func (msg *TxRollbackOk) Id() (uint16, uint16) {
	return ClassTx, MethodTxRollbackOk
}

func (msg *TxRollbackOk) Wait() bool {
	return true
}

func (msg *TxRollbackOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *TxRollbackOk) Read(_ io.Reader) (err error) {
	return nil
}

type ConfirmSelect struct {
	Nowait bool
}

func (msg *ConfirmSelect) Id() (uint16, uint16) {
	return ClassConfirm, MethodConfirmSelect
}

func (msg *ConfirmSelect) Wait() bool {
	return true
}

func (msg *ConfirmSelect) Write(w io.Writer) (err error) {
	var bits byte

	if msg.Nowait {
		bits |= 1 << 0
	}
	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return err
	}

	return nil
}

func (msg *ConfirmSelect) Read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return err
	}
	msg.Nowait = bits&(1<<0) > 0

	return nil
}

type ConfirmSelectOk struct {
}

func (msg *ConfirmSelectOk) Id() (uint16, uint16) {
	return ClassConfirm, MethodConfirmSelectOk
}

func (msg *ConfirmSelectOk) Wait() bool {
	return true
}

func (msg *ConfirmSelectOk) Write(_ io.Writer) (err error) {
	return nil
}

func (msg *ConfirmSelectOk) Read(_ io.Reader) (err error) {
	return nil
}

func (r *Reader) parseMethodFrame(channel uint16, size uint32) (f Frame, err error) {
	mf := &MethodFrame{
		ChannelId: channel,
	}

	if err = binary.Read(r.r, binary.BigEndian, &mf.ClassId); err != nil {
		return nil, err
	}
	if err = binary.Read(r.r, binary.BigEndian, &mf.MethodId); err != nil {
		return nil, err
	}

	switch mf.ClassId {

	case ClassConnection:
		switch mf.MethodId {

		case MethodConnectionStart:
			method := &ConnectionStart{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionStartOk:
			method := &ConnectionStartOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionSecure:
			method := &ConnectionSecure{}
			if err = method.Read(r.r); err != nil {
				return
			}
			mf.Method = method

		case MethodConnectionSecureOk:
			method := &ConnectionSecureOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionTune:
			method := &ConnectionTune{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionTuneOk:
			method := &ConnectionTuneOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionOpen:
			method := &ConnectionOpen{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionOpenOk:
			method := &ConnectionOpenOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionClose:
			method := &ConnectionClose{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionCloseOk:
			method := &ConnectionCloseOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionBlocked:
			method := &ConnectionBlocked{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionUnblocked:
			method := &ConnectionUnblocked{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionUpdateSecret:
			method := &ConnectionUpdateSecret{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConnectionUpdateSecretOk:
			method := &ConnectionUpdateSecretOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case ClassChannel:
		switch mf.MethodId {

		case MethodChannelOpen:
			method := &ChannelOpen{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodChannelOpenOk:
			method := &ChannelOpenOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodChannelFlow:
			method := &ChannelFlow{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodChannelFlowOk:
			method := &ChannelFlowOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodChannelClose:
			method := &ChannelClose{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodChannelCloseOk:
			method := &ChannelCloseOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case ClassExchange:
		switch mf.MethodId {

		case MethodExchangeDeclare:
			method := &ExchangeDeclare{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodExchangeDeclareOk:
			method := &ExchangeDeclareOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodExchangeDelete:
			method := &ExchangeDelete{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodExchangeDeleteOk:
			method := &ExchangeDeleteOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodExchangeBind:
			method := &ExchangeBind{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodExchangeBindOk:
			method := &ExchangeBindOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodExchangeUnbind:
			method := &ExchangeUnbind{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodExchangeUnbindOk:
			method := &ExchangeUnbindOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case ClassQueue:
		switch mf.MethodId {

		case MethodQueueDeclare:
			method := &QueueDeclare{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueueDeclareOk:
			method := &QueueDeclareOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueueBind:
			method := &QueueBind{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueueBindOk:
			method := &QueueBindOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueueUnbind:
			method := &QueueUnbind{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueueUnbindOk:
			method := &QueueUnbindOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueuePurge:
			method := &QueuePurge{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueuePurgeOk:
			method := &QueuePurgeOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueueDelete:
			method := &QueueDelete{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodQueueDeleteOk:
			method := &QueueDeleteOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case ClassBasic:
		switch mf.MethodId {

		case MethodBasicQos:
			method := &BasicQos{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicQosOk:
			method := &BasicQosOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicConsume:
			method := &BasicConsume{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicConsumeOk:
			method := &BasicConsumeOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicCancel:
			method := &BasicCancel{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicCancelOk:
			method := &BasicCancelOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicPublish:
			method := &BasicPublish{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicReturn:
			method := &BasicReturn{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicDeliver:
			method := &BasicDeliver{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicGet:
			method := &BasicGet{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicGetOk:
			method := &BasicGetOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicGetEmpty:
			method := &BasicGetEmpty{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicAck:
			method := &BasicAck{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicReject:
			method := &BasicReject{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicRecoverAsync:
			method := &BasicRecoverAsync{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicRecover:
			method := &BasicRecover{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicRecoverOk:
			method := &BasicRecoverOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodBasicNack:
			method := &BasicNack{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case ClassTx:
		switch mf.MethodId {

		case MethodTxSelect:
			method := &TxSelect{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodTxSelectOk:
			method := &TxSelectOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodTxCommit:
			method := &TxCommit{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodTxCommitOk:
			method := &TxCommitOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodTxRollback:
			method := &TxRollback{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodTxRollbackOk:
			method := &TxRollbackOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case ClassConfirm:
		switch mf.MethodId {

		case MethodConfirmSelect:
			method := &ConfirmSelect{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		case MethodConfirmSelectOk:
			method := &ConfirmSelectOk{}
			if err = method.Read(r.r); err != nil {
				return nil, err
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	default:
		return nil, fmt.Errorf("bad method frame, unknown class %d", mf.ClassId)
	}

	return mf, nil
}
