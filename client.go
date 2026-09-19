package sse

// Client is one subscriber's connection to a topic, created by
// Broker.Subscribe and released by Broker.Unsubscribe.
type Client struct {
	events chan Event
}

func newClient(bufSize int) *Client {
	return &Client{events: make(chan Event, bufSize)}
}

// Events returns the channel the client's published events arrive on.
func (c *Client) Events() <-chan Event {
	return c.events
}

// send delivers e to the client without blocking. It reports whether the
// event was accepted; false means the client's buffer was full.
func (c *Client) send(e Event) bool {
	select {
	case c.events <- e:
		return true
	default:
		return false
	}
}
