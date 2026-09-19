package sse

import (
	"io"
	"net/http"
	"time"
)

const defaultHeartbeat = 15 * time.Second

// TopicFunc extracts the topic a request subscribes to.
type TopicFunc func(*http.Request) string

// Handler streams a Broker topic's events to an HTTP client as
// Server-Sent Events.
type Handler struct {
	broker    *Broker
	topicFunc TopicFunc
	heartbeat time.Duration
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithHeartbeat sets the interval between keep-alive comment lines. Zero
// disables heartbeats. The default is 15 seconds.
func WithHeartbeat(d time.Duration) HandlerOption {
	return func(h *Handler) { h.heartbeat = d }
}

// NewHandler creates a Handler that streams events from broker, using
// topicFunc to determine which topic each request subscribes to.
func NewHandler(broker *Broker, topicFunc TopicFunc, opts ...HandlerOption) *Handler {
	h := &Handler{
		broker:    broker,
		topicFunc: topicFunc,
		heartbeat: defaultHeartbeat,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// ServeHTTP implements http.Handler. It blocks for the lifetime of the
// connection, returning when the request context is canceled.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	topic := h.topicFunc(r)
	client := h.broker.Subscribe(topic)
	defer h.broker.Unsubscribe(topic, client)

	header := w.Header()
	header.Set("Content-Type", "text/event-stream; charset=utf-8")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for _, e := range h.broker.Replay(topic, r.Header.Get("Last-Event-ID")) {
		if err := e.encode(w); err != nil {
			return
		}
	}
	flusher.Flush()

	var tick <-chan time.Time
	if h.heartbeat > 0 {
		ticker := time.NewTicker(h.heartbeat)
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-client.Events():
			if err := e.encode(w); err != nil {
				return
			}
			flusher.Flush()
		case <-tick:
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
