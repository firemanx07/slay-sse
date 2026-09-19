// Package sse implements Server-Sent Events: a Broker for topic-based
// publish/subscribe and an http.Handler that streams events to subscribed
// clients per the SSE wire format.
package sse

import "sync"

const defaultClientBuffer = 16

// DropFunc is called when a client's buffer is full and an event is
// dropped for it. It must not block or call back into the Broker.
type DropFunc func(topic string, e Event)

// Broker fans published events out to the clients subscribed to each
// topic. The zero value is not usable; construct one with NewBroker.
type Broker struct {
	mu           sync.RWMutex
	topics       map[string]map[*Client]struct{}
	clientBuffer int
	replay       ReplayStore
	onDrop       DropFunc
}

// Option configures a Broker.
type Option func(*Broker)

// WithClientBuffer sets the per-client event buffer size. The default is 16.
func WithClientBuffer(n int) Option {
	return func(b *Broker) { b.clientBuffer = n }
}

// WithReplayStore attaches a ReplayStore used to serve clients that
// reconnect with a Last-Event-ID.
func WithReplayStore(r ReplayStore) Option {
	return func(b *Broker) { b.replay = r }
}

// WithDropFunc sets the callback invoked when a slow client's buffer is
// full and an event is dropped for it.
func WithDropFunc(f DropFunc) Option {
	return func(b *Broker) { b.onDrop = f }
}

// NewBroker creates a Broker ready to accept subscriptions and publishes.
func NewBroker(opts ...Option) *Broker {
	b := &Broker{
		topics:       make(map[string]map[*Client]struct{}),
		clientBuffer: defaultClientBuffer,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Subscribe registers a new client on topic and returns it. Callers must
// call Unsubscribe when the client disconnects.
func (b *Broker) Subscribe(topic string) *Client {
	c := newClient(b.clientBuffer)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.topics[topic] == nil {
		b.topics[topic] = make(map[*Client]struct{})
	}
	b.topics[topic][c] = struct{}{}
	return c
}

// Unsubscribe removes a client from topic. It is safe to call more than
// once for the same client.
func (b *Broker) Unsubscribe(topic string, c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	clients := b.topics[topic]
	if clients == nil {
		return
	}
	delete(clients, c)
	if len(clients) == 0 {
		delete(b.topics, topic)
	}
}

// Publish fans e out to every client currently subscribed to topic. A
// client whose buffer is full has e dropped for it rather than blocking
// Publish for the other subscribers; see WithDropFunc.
func (b *Broker) Publish(topic string, e Event) {
	if b.replay != nil {
		b.replay.Add(topic, e)
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for c := range b.topics[topic] {
		if !c.send(e) && b.onDrop != nil {
			b.onDrop(topic, e)
		}
	}
}

// Replay returns the events recorded for topic after lastEventID, oldest
// first. It returns nil if no ReplayStore is configured.
func (b *Broker) Replay(topic, lastEventID string) []Event {
	if b.replay == nil {
		return nil
	}
	return b.replay.Since(topic, lastEventID)
}
