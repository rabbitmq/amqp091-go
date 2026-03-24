# Design Document: Channel Deadlock Fix

**References**: [Issue #253](https://github.com/rabbitmq/amqp091-go/issues/253), [Issue #225](https://github.com/rabbitmq/amqp091-go/issues/225)

**Breaking changes**: Allowed and required.

---

## Summary

The current `amqp091-go` channel implementation has a structural deadlock problem: the connection's single reader goroutine calls `channel.recv()` and `dispatch()` synchronously, and multiple code paths inside `dispatch()` can block indefinitely. This stalls frame processing for every channel on the connection.

This document describes a four-part redesign that eliminates all identified deadlock vectors:

1. **Per-channel goroutine** — decouple the reader from dispatch
2. **Per-RPC one-shot response channel** — replace the shared `ch.rpc` channel
3. **Context support with close-on-timeout** — add `context.Context` to all RPC methods
4. **Library-owned non-blocking notification channels** — remove user-provided unbuffered channel hazard

---

## Root Cause Analysis

### Deadlock Scenario 1: Blocked RPC (Issues #253, #225)

1. User goroutine calls `ch.call()` (e.g. `QueueUnbind`), which sends a method frame and then blocks in a `select` waiting on `ch.rpc` or `ch.errors`.
2. The server is blocked (OOM, connection blocked, network partition) and never sends a reply.
3. The `ch.rpc` channel (unbuffered) never receives, so `ch.call()` blocks forever.
4. There is no timeout, no context cancellation — the caller is stuck.

### Deadlock Scenario 2: Notify channel backpressure

1. User registers `NotifyClose(make(chan *Error))` with an **unbuffered** channel.
2. The connection reader goroutine calls `c.shutdown()` → tries to send on `c.closes` channels: `c <- err`.
3. If nobody is reading from that unbuffered channel, the reader goroutine **blocks forever**.
4. This prevents `close(c.errors)` from ever executing, so any goroutine waiting in `ch.call()` on `ch.errors` also never unblocks.

### Deadlock Scenario 3: `dispatch()` blocks the reader

The `dispatch()` method runs **inline on the connection reader goroutine**. Several paths can block:

- The `default` case sends on `ch.rpc` (unbuffered). If the user goroutine that should be receiving on `ch.rpc` has already given up or hasn't started listening yet, the reader goroutine blocks, stalling **all** channels on the connection.
- `ch.flows`, `ch.cancels`, `ch.returns` channels — if the user doesn't read from them, the reader blocks.
- `ch.consumers.send()` holds a mutex and sends on a channel — if the consumer buffer goroutine is slow, the reader blocks.

### Deadlock Scenario 4: `dispatch` ↔ `send` circular dependency

1. `dispatch()` handles `channelClose`. It acquires `ch.m.Lock()`, then calls `ch.send()` → `ch.sendClosed()` → `c.send()` → acquires `c.sendM.Lock()` and writes to the socket.
2. If the socket write blocks (TCP backpressure, server blocked), the reader goroutine is stuck holding `ch.m`.
3. Meanwhile, `PublishWithDeferredConfirm` also acquires `ch.m.Lock()`. Both paths contend on `ch.m` and the connection write path.

---

## Architecture

```
Current: Reader blocks on dispatch
────────────────────────────────────────────────────────
Connection Reader ──(inline call)──▶ channel.recv()
                                          │
                                          ▼
                                    channel.dispatch()
                                          │
                              ┌───────────┴───────────┐
                              ▼                       ▼
                    ch.rpc (unbuffered)      notify channels
                    [BLOCKS reader]          [BLOCKS reader]


Proposed: Reader never blocks
────────────────────────────────────────────────────────
Connection Reader ──(non-blocking send)──▶ ch.frames (buffered, cap 64)
                                                  │
                                                  ▼
                                      ch.recvLoop goroutine
                                                  │
                                                  ▼
                                          channel.dispatch()
                                                  │
                                  ┌───────────────┴───────────────┐
                                  ▼                               ▼
                    ch.pendingRPC (buffered,           notify channels
                    cap 1, one-shot per RPC)           (library-owned,
                    [never blocks]                      buffered, cap 1)
                                                       [never blocks]
```

---

## Part 1: Per-channel Goroutine

**Goal**: Decouple the connection reader from channel frame processing.

### `Channel` struct changes

Remove:
- `recv func(*Channel, frame)` — function-pointer state machine field
- `rpc chan message` — shared RPC response channel

Add:
- `frames chan frame` — buffered (cap 64) frame inbox; the reader sends here
- `pendingRPC chan message` — per-RPC one-shot response channel (nil when idle)
- `state func(*Channel, frame)` — replaces `recv` for state tracking

### `newChannel` changes

```go
func newChannel(c *Connection, id uint16) *Channel {
    ch := &Channel{
        connection: c,
        id:         id,
        consumers:  makeConsumers(),
        confirms:   newConfirms(),
        frames:     make(chan frame, 64),
        errors:     make(chan *Error, 1),
        close:      make(chan struct{}),
        state:      (*Channel).recvMethod,
    }
    go ch.recvLoop()
    return ch
}
```

### `recvLoop` goroutine

```go
func (ch *Channel) recvLoop() {
    for f := range ch.frames {
        ch.state(ch, f)
    }
}
```

When `ch.frames` is closed (during shutdown), the goroutine exits cleanly.

### `shutdown` changes

`close(ch.frames)` must be called early in `shutdown()` to stop the channel goroutine. All sends to `ch.closes` and `ch.errors` become non-blocking (Part 4).

```go
func (ch *Channel) shutdown(e *Error) {
    ch.setClosed()
    ch.destructor.Do(func() {
        ch.m.Lock()
        defer ch.m.Unlock()
        ch.notifyM.Lock()
        defer ch.notifyM.Unlock()

        close(ch.frames) // stop the recvLoop goroutine

        if e != nil {
            for _, c := range ch.closes {
                select { case c <- e: default: }
            }
            select { case ch.errors <- e: default: }
        }

        ch.consumers.close()

        for _, c := range ch.closes  { close(c) }
        for _, c := range ch.flows   { close(c) }
        for _, c := range ch.returns { close(c) }
        for _, c := range ch.cancels { close(c) }

        ch.flows = nil; ch.closes = nil
        ch.returns = nil; ch.cancels = nil

        if ch.confirms != nil { ch.confirms.Close() }

        close(ch.errors)
        close(ch.close)
        ch.noNotify = true
    })
}
```

### State machine functions

`transition()` is removed. `recvMethod`, `recvHeader`, `recvContent` directly assign `ch.state`:

```go
func (ch *Channel) recvMethod(f frame) {
    switch frame := f.(type) {
    case *methodFrame:
        if msg, ok := frame.Method.(messageWithContent); ok {
            ch.body = make([]byte, 0)
            ch.message = msg
            ch.state = (*Channel).recvHeader
            return
        }
        ch.dispatch(frame.Method)
        ch.state = (*Channel).recvMethod
    case *headerFrame:
        ch.state = (*Channel).recvMethod
    case *bodyFrame:
        ch.state = (*Channel).recvMethod
    }
}
```

### `Connection.dispatchN` changes

Replace inline `channel.recv(channel, f)` with a non-blocking send:

```go
func (c *Connection) dispatchN(f frame) {
    c.m.Lock()
    channel, ok := c.channels[f.channel()]
    if ok {
        updateChannel(f, channel)
    }
    c.m.Unlock()

    if ok {
        select {
        case channel.frames <- f:
        default:
            // Frame buffer full — channel goroutine is stuck.
            // Close the channel asynchronously; reader is unblocked.
            go c.closeChannel(channel, &Error{
                Code:   ChannelError,
                Reason: "channel frame buffer overflow",
            })
        }
    } else {
        c.dispatchClosed(f)
    }
}
```

The reader goroutine can never block on channel processing.

---

## Part 2: Per-RPC One-Shot Response Channel

**Goal**: Replace the shared `ch.rpc` channel with a per-call buffered channel, so a stale response after a timeout cannot poison the next RPC and `dispatch()` never blocks on RPC delivery.

### `call()` changes

```go
func (ch *Channel) call(ctx context.Context, req message, res ...message) error {
    if err := ch.send(req); err != nil {
        return err
    }
    if !req.wait() {
        return nil
    }

    // One-shot, buffered: dispatch can always send without blocking.
    reply := make(chan message, 1)

    ch.m.Lock()
    ch.pendingRPC = reply
    ch.m.Unlock()

    defer func() {
        ch.m.Lock()
        if ch.pendingRPC == reply {
            ch.pendingRPC = nil
        }
        ch.m.Unlock()
    }()

    select {
    case <-ctx.Done():
        go ch.connection.closeChannel(ch, &Error{
            Code:   ChannelError,
            Reason: "context deadline exceeded during RPC",
        })
        return ctx.Err()
    case e, ok := <-ch.errors:
        if ok { return e }
        return ErrClosed
    case msg := <-reply:
        if msg == nil { return ErrClosed }
        for _, try := range res {
            if reflect.TypeOf(msg) == reflect.TypeOf(try) {
                reflect.ValueOf(try).Elem().Set(reflect.ValueOf(msg).Elem())
                return nil
            }
        }
        return ErrCommandInvalid
    }
}
```

### `dispatch()` default case

```go
default:
    ch.m.Lock()
    pending := ch.pendingRPC
    ch.m.Unlock()

    if pending != nil {
        select {
        case pending <- msg:
        default:
        }
    }
    // If pending is nil, nobody is waiting — discard the message.
```

### Why this is safe after a timeout

When the context fires:

1. `call()` hits `case <-ctx.Done()`.
2. The `defer` runs: sets `ch.pendingRPC = nil`.
3. `call()` triggers `closeChannel` and returns `ctx.Err()`.

If the stale response arrives later:

4. `dispatch()` reads `ch.pendingRPC` — it's `nil`.
5. The response is discarded. No channel is blocked. No state is corrupted.

If the stale response arrives before the `defer` has run (race window):

4. `dispatch()` reads `ch.pendingRPC` — it's still `reply`.
5. `dispatch()` sends on `reply` (succeeds because buffer cap is 1).
6. The `defer` runs and sets `ch.pendingRPC = nil`.
7. Nobody reads from `reply`. It is garbage collected.

The buffer of 1 is what makes this safe — `dispatch()` **never blocks**.

---

## Part 3: Context Support with Close-on-Timeout

**Goal**: Allow callers to set timeouts and cancellations on all RPC operations. When the context expires, the AMQP channel is closed — it is in an indeterminate state and cannot be reused.

> **Why close and not reset?** Closing is the only safe semantic. If `QueueDeclare` timed out but the server processed it, a `basic.consume` on the undeclared queue from the caller's perspective would appear to succeed on the server but fail in the client. AMQP requires that synchronous RPCs on a channel are fully serialized. Closing the channel eliminates the ambiguity.

### Public method signature changes

Every public RPC method gains `ctx context.Context` as its first parameter. The `XxxWithContext` duplicates are removed.

| Old | New |
|---|---|
| `Close() error` | `Close(ctx context.Context) error` |
| `Qos(prefetchCount, prefetchSize int, global bool) error` | `Qos(ctx context.Context, ...) error` |
| `QueueDeclare(name string, ...) (Queue, error)` | `QueueDeclare(ctx context.Context, ...) (Queue, error)` |
| `QueueBind(name, key, exchange string, ...) error` | `QueueBind(ctx context.Context, ...) error` |
| `QueueUnbind(name, key, exchange string, ...) error` | `QueueUnbind(ctx context.Context, ...) error` |
| `QueueDelete(name string, ...) (int, error)` | `QueueDelete(ctx context.Context, ...) (int, error)` |
| `QueuePurge(name string, noWait bool) (int, error)` | `QueuePurge(ctx context.Context, ...) (int, error)` |
| `Consume(queue, consumer string, ...) (<-chan Delivery, error)` | `Consume(ctx context.Context, ...) (<-chan Delivery, error)` |
| `ConsumeWithContext(ctx, queue, consumer, ...) (<-chan Delivery, error)` | **Removed** (merged into `Consume`) |
| `ExchangeDeclare(name, kind string, ...) error` | `ExchangeDeclare(ctx context.Context, ...) error` |
| `ExchangeBind(destination, key, source string, ...) error` | `ExchangeBind(ctx context.Context, ...) error` |
| `ExchangeUnbind(destination, key, source string, ...) error` | `ExchangeUnbind(ctx context.Context, ...) error` |
| `ExchangeDelete(name string, ...) error` | `ExchangeDelete(ctx context.Context, ...) error` |
| `Publish(exchange, key string, ...) error` | `Publish(ctx context.Context, ...) error` |
| `PublishWithContext(ctx, exchange, key string, ...) error` | **Removed** (merged into `Publish`) |
| `PublishWithDeferredConfirm(exchange, key string, ...) (*DeferredConfirmation, error)` | `PublishWithDeferredConfirm(ctx context.Context, ...) (*DeferredConfirmation, error)` |
| `PublishWithDeferredConfirmWithContext(ctx, ...) (*DeferredConfirmation, error)` | **Removed** (merged) |
| `Get(queue string, autoAck bool) (msg Delivery, ok bool, err error)` | `Get(ctx context.Context, ...) (Delivery, bool, error)` |
| `Ack/Nack/Reject` | No change (no server round-trip) |
| `Confirm(noWait bool) error` | `Confirm(ctx context.Context, noWait bool) error` |
| `Flow(active bool) error` | `Flow(ctx context.Context, active bool) error` |
| `Recover(requeue bool) error` | `Recover(ctx context.Context, requeue bool) error` |
| `Tx/TxCommit/TxRollback` | `Tx(ctx)/TxCommit(ctx)/TxRollback(ctx)` |
| `Cancel(consumer string, noWait bool) error` | `Cancel(ctx context.Context, consumer string, noWait bool) error` |

`Publish` and `PublishWithDeferredConfirm` are fire-and-forget (no server round-trip). The context gates entry only:

```go
func (ch *Channel) Publish(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
    }
    _, err := ch.publishWithDeferredConfirm(exchange, key, mandatory, immediate, msg)
    return err
}
```

### Connection API changes

```go
// Channel now takes a context; open() will not block forever.
func (c *Connection) Channel(ctx context.Context) (*Channel, error)

// Close replaces CloseDeadline; context subsumes deadline semantics.
func (c *Connection) Close(ctx context.Context) error

// UpdateSecret gets context support.
func (c *Connection) UpdateSecret(ctx context.Context, newSecret, reason string) error
```

`CloseDeadline` is removed. Callers use `context.WithDeadline` or `context.WithTimeout` instead.

---

## Part 4: Library-Owned Non-Blocking Notification Channels

**Goal**: Eliminate the class of deadlocks caused by user-provided unbuffered notification channels.

**Approach**: `Notify*` methods no longer accept a channel. They allocate and return a buffered, receive-only channel. All internal sends to notification channels use `select/default` (non-blocking).

### Channel `Notify*` signatures

```go
func (ch *Channel) NotifyClose() <-chan *Error         // was: NotifyClose(c chan *Error) chan *Error
func (ch *Channel) NotifyFlow() <-chan bool             // was: NotifyFlow(c chan bool) chan bool
func (ch *Channel) NotifyReturn() <-chan Return         // was: NotifyReturn(c chan Return) chan Return
func (ch *Channel) NotifyCancel() <-chan string         // was: NotifyCancel(c chan string) chan string
func (ch *Channel) NotifyPublish() <-chan Confirmation  // was: NotifyPublish(confirm chan Confirmation) chan Confirmation
```

`NotifyConfirm` is **removed**. It spawned a goroutine to fan out to `ack`/`nack` channels; users should use `NotifyPublish()` and branch on `Confirmation.Ack`.

Internal channel allocations:

```go
func (ch *Channel) NotifyClose() <-chan *Error {
    ch.notifyM.Lock()
    defer ch.notifyM.Unlock()
    c := make(chan *Error, 1)
    if ch.noNotify {
        close(c)
    } else {
        ch.closes = append(ch.closes, c)
    }
    return c
}
```

Same pattern for all other `Notify*` methods. `NotifyPublish` uses `make(chan Confirmation, 32)`.

### Connection `Notify*` signatures

```go
func (c *Connection) NotifyClose() <-chan *Error    // was: NotifyClose(receiver chan *Error) chan *Error
func (c *Connection) NotifyBlocked() <-chan Blocking // was: NotifyBlocked(receiver chan Blocking) chan Blocking
```

### Non-blocking sends throughout

Every send to a notification channel becomes:

```go
select {
case c <- value:
default:
}
```

Affected locations:
- `channel.go` `dispatch()`: `channelFlow`, `basicCancel`, `basicReturn` cases
- `channel.go` `shutdown()`: `ch.closes`, `ch.errors` sends
- `connection.go` `shutdown()`: `c.closes`, `c.errors` sends
- `connection.go` `dispatch0()`: `connectionBlocked`, `connectionUnblocked` sends
- `confirms.go` `confirm()`: listener channel sends

---

## What Gets Removed

| Item | Reason |
|---|---|
| `recv func(*Channel, frame)` field | Replaced by `state` + `recvLoop` goroutine |
| `rpc chan message` field | Replaced by `pendingRPC` one-shot channel |
| `transition()` method | Direct `ch.state` assignment |
| `ConsumeWithContext` | Merged into `Consume(ctx, ...)` |
| `PublishWithContext` | Merged into `Publish(ctx, ...)` |
| `PublishWithDeferredConfirmWithContext` | Merged into `PublishWithDeferredConfirm(ctx, ...)` |
| `CloseDeadline` on Connection | Replaced by `Close(ctx)` |
| `NotifyConfirm` | Use `NotifyPublish` + `Confirmation.Ack` |
| User-provided channels in `Notify*` | Library allocates them |

## What Gets Added

| Item | Purpose |
|---|---|
| `frames chan frame` (cap 64) on Channel | Buffered inbox decoupling reader from dispatch |
| `recvLoop()` goroutine per channel | Processes frames independently from reader |
| `pendingRPC chan message` (cap 1) on Channel | Per-RPC one-shot response slot |
| `context.Context` on all RPC methods | Timeout and cancellation support |
| Non-blocking sends everywhere | Eliminates all notify-channel deadlock vectors |
| Frame-buffer overflow protection in `dispatchN` | Handles pathologically slow channel goroutines |

## What Stays the Same

- The state machine logic (`recvMethod`, `recvHeader`, `recvContent`) — same transitions, same frame reassembly
- The `consumers` buffering mechanism — already properly decoupled via goroutines
- The connection reader goroutine — same loop, `dispatchN` is now non-blocking
- The heartbeater — unchanged
- `sendOpen` / `sendClosed` / `send` on Channel — unchanged
- `DeferredConfirmation` type and `Wait`/`WaitContext` — unchanged
- Publisher confirms tracking (`confirms` struct) — unchanged
- `Connection.call()` and the connection-level `c.rpc` channel — connection handshake is fine as-is

---

## Deadlock Scenario Traces (Post-Fix)

### RPC during connection blocked (Issue #253)

1. User calls `ch.QueueUnbind(ctx, ...)`.
2. `call()` sends the method frame, creates `reply := make(chan message, 1)`, sets `ch.pendingRPC = reply`.
3. Server is blocked. Never sends a response.
4. Context expires. `call()` hits `case <-ctx.Done()`.
5. `call()` launches `go ch.connection.closeChannel(ch, ...)` and returns `ctx.Err()`.
6. `closeChannel` calls `ch.shutdown(err)` then `c.releaseChannel(ch)`.
7. `shutdown` closes `ch.frames` (stopping the goroutine), non-blocking sends to `ch.errors` and notify channels.
8. If the stale response ever arrives, `dispatchN` doesn't find the channel in `c.channels` and calls `dispatchClosed`.
9. **No deadlock.** User gets a context error and opens a new channel.

### `Connection.Channel()` with network down (Issue #225)

1. User calls `conn.Channel(ctx)`.
2. `openChannel` allocates a channel, calls `ch.open(ctx)`.
3. `ch.open` calls `ch.call(ctx, &channelOpen{}, &channelOpenOk{})`.
4. The frame is sent. No response comes.
5. Context expires. `call()` returns `ctx.Err()`. Channel is closed.
6. `openChannel` calls `c.releaseChannel(ch)`. Channel is cleaned up.
7. **No deadlock.** User gets a context error.

### Unbuffered `NotifyClose` during shutdown

Eliminated entirely. The library owns channel allocation with buffer 1. Non-blocking sends ensure `shutdown()` never blocks even if the buffer is full.

### `dispatch()` blocking the reader goroutine

Eliminated. `dispatch()` now runs on the per-channel goroutine, not the reader. The reader only does a non-blocking `channel.frames <- f`. Within the channel goroutine, all sends are non-blocking: RPC responses go to a buffered one-shot channel, notify sends use `select/default`, consumer sends use the existing buffered mechanism.

---

## Implementation Plan

### Step 1: `channel.go` — Channel struct and core infrastructure
- Remove `recv func(*Channel, frame)` and `rpc chan message`
- Add `frames chan frame`, `pendingRPC chan message`, `state func(*Channel, frame)`
- Update `newChannel`: initialize new fields, launch `recvLoop`
- Add `recvLoop()` method
- Update `shutdown()`: close `ch.frames`, non-blocking notification sends
- Update `recvMethod`, `recvHeader`, `recvContent`: assign `ch.state`, remove `transition()`

### Step 2: `connection.go` — Non-blocking `dispatchN`
- Replace inline `channel.recv(channel, f)` with `select { case channel.frames <- f: default: go closeChannel(...) }`

### Step 3: `channel.go` — `call()` rewrite
- New signature: `call(ctx context.Context, req message, res ...message) error`
- Per-RPC `reply := make(chan message, 1)`
- Store in / clear from `ch.pendingRPC` under `ch.m`
- `select` on `ctx.Done()`, `ch.errors`, `reply`
- On context expiry: trigger `closeChannel`

### Step 4: `channel.go` — `dispatch()` default case
- Read `ch.pendingRPC` under `ch.m.Lock()`
- Non-blocking send to it; discard if nil

### Step 5: `channel.go` and `connection.go` — Public API context changes
- Add `ctx context.Context` to every public RPC method
- Remove `XxxWithContext` duplicates
- Update `Channel.open`, `Connection.Channel`, `Connection.Close`, `Connection.UpdateSecret`
- Remove `CloseDeadline`

### Step 6: `channel.go` and `connection.go` — Notify API overhaul
- Change all `Notify*` methods to allocate and return `<-chan` channels
- Remove `NotifyConfirm`
- Make all notification sends non-blocking throughout `dispatch()`, `shutdown()`, `confirms.go`

### Step 7: Tests
- Update `client_test.go`, `connection_test.go`, `examples_test.go`, `example_client_test.go` for new API
- Update `integration_test.go`
- Update `_examples/` directory
- Remove tests for removed API (`NotifyConfirm`, `XxxWithContext` variants)
- Add new tests:
  - `TestCallContextTimeout` — context expiry closes the channel
  - `TestDispatchNOverflow` — frame buffer overflow closes the channel
  - `TestShutdownNonBlocking` — shutdown completes with no listeners

### Step 8: Documentation
- Update `doc.go` with new API patterns and context semantics
- Remove warnings about buffered notify channels (library handles this now)
- Document close-on-timeout semantics
