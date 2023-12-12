package amqp091

/*
NotifyMiddlewares is a function for notify middlewares. Each Notify function, give middlewares
and run them in order after the notify function is called.
*/
type NotifyMiddlewares func(sc *SubChannel)

/*
SubChannel is a channel for a connection. For security, the user can modify the channel parameters that
in the SubChannel.
*/
type SubChannel struct {
	connection *Connection

	rpc       chan message
	consumers *consumers

	id uint16

	// Selects on any errors from shutdown during RPC
	errors chan *Error

	// Current state for frame re-assembly, only mutated from recv
	message messageWithContent
	header  *headerFrame
	body    []byte
}
