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

// LifeCycle is the lifecycle of the connection or channel.
type LifeCycle struct {
	state           ILifeCycleState    // The current state of the connection or channel.
	chStatusChanged chan *StateChanged // The channel to send the state changes to.
	mutex           *sync.Mutex        // The mutex to protect the state changes.
	queue           []*StateChanged    // Queue to hold state transitions for sequential FIFO delivery.
	sending         bool               // True if deliverLoop is currently active.
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

	if l.chStatusChanged == nil {
		return
	}

	sc := &StateChanged{
		From: oldState,
		To:   value,
	}

	l.queue = append(l.queue, sc)
	if !l.sending {
		l.sending = true
		go l.deliverLoop()
	}
}

func (l *LifeCycle) deliverLoop() {
	for {
		l.mutex.Lock()
		if len(l.queue) == 0 || l.chStatusChanged == nil {
			l.sending = false
			l.mutex.Unlock()
			return
		}
		sc := l.queue[0]
		l.queue = l.queue[1:]
		ch := l.chStatusChanged
		l.mutex.Unlock()

		ch <- sc
	}
}

func (l *LifeCycle) notifyStateChange(channel chan *StateChanged) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.chStatusChanged = channel
}
