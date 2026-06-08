package amqp091

import (
	"fmt"
	"sync"
)

const (
	stateOpen         = iota
	stateReconnecting = iota
	stateClosing      = iota
	stateClosed       = iota
)

// ILifeCycleState defines the connection or channel state
// see the iota constants for possible states: open, reconnecting, closing, closed
type ILifeCycleState interface {
	getState() int
}

type StateOpen struct {
}

func (o *StateOpen) getState() int {
	return stateOpen
}

type StateReconnecting struct {
}

func (r *StateReconnecting) getState() int {
	return stateReconnecting
}

type StateClosing struct {
}

func (c *StateClosing) getState() int {
	return stateClosing
}

type StateClosed struct {
	error error
}

func (c *StateClosed) GetError() error {
	return c.error
}

func (c *StateClosed) getState() int {
	return stateClosed
}

func statusToString(status ILifeCycleState) string {
	switch status.getState() {
	case stateOpen:
		return "open"
	case stateReconnecting:
		return "reconnecting"
	case stateClosing:
		return "closing"
	case stateClosed:
		return "closed"
	}
	return "unknown"

}

// StateChanged defines the connection or channel life cycle.
// See ILifeCycleState for more details about the possible states.
// Every time the state changes,
// a StateChanged struct is sent to the channel defined in the
// NotifyStateChange method.
type StateChanged struct {
	From ILifeCycleState
	To   ILifeCycleState
}

func (s StateChanged) String() string {
	switch s.To.(type) {
	case *StateClosed:
		if s.To.(*StateClosed).error == nil {
			return fmt.Sprintf("From: %s, To: %s", statusToString(s.From), statusToString(s.To))
		}
		return fmt.Sprintf("From: %s, To: %s, Error: %s", statusToString(s.From), statusToString(s.To), s.To.(*StateClosed).error)

	}
	return fmt.Sprintf("From: %s, To: %s", statusToString(s.From), statusToString(s.To))
}

type stateListener struct {
	ch      chan *StateChanged
	queue   []*StateChanged
	sending bool
}

// enqueue appends a state change to the listener's bounded queue using a sliding window.
// If the queue size exceeds maxQueueSize, the oldest state change is dropped.
// This assumes the caller holds the LifeCycle mutex.
func (sl *stateListener) enqueue(sc *StateChanged) {
	const maxQueueSize = 50
	if len(sl.queue) < maxQueueSize {
		sl.queue = append(sl.queue, sc)
	} else {
		sl.queue = append(sl.queue[1:], sc)
	}
}

// LifeCycle is the lifecycle of the connection or channel.
//
// The listener framework manages state change notifications for registered channels.
// Key characteristics:
//   - Multiple listeners can register concurrently via NotifyStateChange.
//   - Each listener operates concurrently and is isolated from others; a blocked or slow
//     listener will not block other listeners or the main SetState() execution.
//   - Strict FIFO ordering of state transitions is guaranteed for each listener by spawning
//     at most one dedicated delivery goroutine per listener.
//   - Memory usage is strictly bounded by a sliding-window queue per listener (maxQueueSize = 50).
//   - Listener channels are cleanly closed as soon as the final StateClosed transition is sent.
type LifeCycle struct {
	state     ILifeCycleState  // The current state of the connection or channel.
	listeners []*stateListener // The registered state change listeners.
	mutex     *sync.Mutex      // The mutex to protect the state changes.
}

func NewLifeCycle() *LifeCycle {
	return &LifeCycle{
		state: &StateClosed{},
		mutex: &sync.Mutex{},
	}
}

func (l *LifeCycle) State() ILifeCycleState {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.state
}

func (l *LifeCycle) SetState(value ILifeCycleState) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.state == value {
		return
	}

	oldState := l.state
	l.state = value

	sc := &StateChanged{
		From: oldState,
		To:   value,
	}

	for _, listener := range l.listeners {
		listener.enqueue(sc)
		if !listener.sending {
			listener.sending = true
			go l.deliverToListener(listener)
		}
	}
}

func (l *LifeCycle) deliverToListener(listener *stateListener) {
	for {
		l.mutex.Lock()
		if len(listener.queue) == 0 {
			listener.sending = false
			l.mutex.Unlock()
			return
		}
		sc := listener.queue[0]
		listener.queue = listener.queue[1:]
		ch := listener.ch
		l.mutex.Unlock()

		ch <- sc

		// If the transition is to StateClosed, this is the terminal state.
		// We can safely close the channel now because:
		// 1. No more states will be appended (StateClosed is final).
		// 2. The StateClosed notification was just successfully sent.
		if _, ok := sc.To.(*StateClosed); ok {
			l.mutex.Lock()
			close(ch)
			l.removeListener(listener)
			l.mutex.Unlock()
			return
		}
	}
}

func (l *LifeCycle) removeListener(listener *stateListener) {
	for i, lis := range l.listeners {
		if lis == listener {
			l.listeners = append(l.listeners[:i], l.listeners[i+1:]...)
			break
		}
	}
}

func (l *LifeCycle) notifyStateChange(channel chan *StateChanged) {
	if channel == nil {
		return
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// If the connection/channel is already closed, close the channel immediately.
	if _, ok := l.state.(*StateClosed); ok {
		close(channel)
		return
	}

	// Prevent duplicate registration of the same channel.
	for _, lis := range l.listeners {
		if lis.ch == channel {
			return
		}
	}

	listener := &stateListener{
		ch: channel,
	}
	l.listeners = append(l.listeners, listener)
}
