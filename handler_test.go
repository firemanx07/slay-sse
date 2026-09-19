package sse

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncRecorder is an http.ResponseWriter safe for the concurrent
// write-while-read pattern these tests need: the handler writes from its
// own goroutine while the test polls the body from the calling goroutine.
// httptest.ResponseRecorder's bytes.Buffer is not safe for that.
type syncRecorder struct {
	mu     sync.Mutex
	header http.Header
	body   bytes.Buffer
}

// newSyncRecorder creates a syncRecorder ready to receive a response.
func newSyncRecorder() *syncRecorder {
	return &syncRecorder{header: make(http.Header)}
}

// Header implements http.ResponseWriter.
func (r *syncRecorder) Header() http.Header { return r.header }

// Write implements http.ResponseWriter, appending to the response body.
func (r *syncRecorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.Write(p)
}

// WriteHeader implements http.ResponseWriter. The status code isn't
// recorded; these tests only assert on the streamed body and headers.
func (r *syncRecorder) WriteHeader(int) {}

// Flush implements http.Flusher as a no-op; writes are visible immediately.
func (r *syncRecorder) Flush() {}

// String returns the response body written so far.
func (r *syncRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.String()
}

// TestHandlerStreamsPublishedEvents verifies a published event reaches a
// streaming client's response body, and the handler returns once the
// request context is canceled.
func TestHandlerStreamsPublishedEvents(t *testing.T) {
	broker := NewBroker()
	h := NewHandler(broker, func(*http.Request) string { return "topic" }, WithHeartbeat(0))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	waitForSubscriber(t, broker, "topic")
	broker.Publish("topic", Event{ID: "1", Data: "hello"})
	waitForBody(t, rec, "data: hello\n\n")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after context cancel")
	}

	if got := rec.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}
}

// TestHandlerReplaysFromLastEventID verifies the handler replays only the
// events recorded after the request's Last-Event-ID header.
func TestHandlerReplaysFromLastEventID(t *testing.T) {
	broker := NewBroker(WithReplayStore(NewMemoryReplayStore(10)))
	broker.Publish("topic", Event{ID: "1", Data: "a"})
	broker.Publish("topic", Event{ID: "2", Data: "b"})

	h := NewHandler(broker, func(*http.Request) string { return "topic" }, WithHeartbeat(0))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)
	req.Header.Set("Last-Event-ID", "1")
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	waitForBody(t, rec, "data: b\n\n")
	cancel()
	<-done

	if strings.Contains(rec.String(), "data: a\n\n") {
		t.Errorf("replay included event at or before Last-Event-ID: %q", rec.String())
	}
}

// TestHandlerUnsubscribesOnDisconnect verifies the handler removes its
// client from the broker's topic once the request context is canceled.
func TestHandlerUnsubscribesOnDisconnect(t *testing.T) {
	broker := NewBroker()
	h := NewHandler(broker, func(*http.Request) string { return "topic" }, WithHeartbeat(0))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	waitForSubscriber(t, broker, "topic")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after context cancel")
	}

	broker.mu.RLock()
	n := len(broker.topics["topic"])
	broker.mu.RUnlock()
	if n != 0 {
		t.Fatalf("got %d subscribers left on topic, want 0", n)
	}
}

// TestHandlerRejectsNonFlushingWriter verifies the handler responds with
// 500 when the ResponseWriter doesn't implement http.Flusher.
func TestHandlerRejectsNonFlushingWriter(t *testing.T) {
	broker := NewBroker()
	h := NewHandler(broker, func(*http.Request) string { return "topic" })

	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	rec := httptest.NewRecorder()
	var w http.ResponseWriter = nonFlushingWriter{rec}

	h.ServeHTTP(w, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

// TestHandlerSendsHeartbeat verifies the handler writes a heartbeat comment
// line at the configured interval.
func TestHandlerSendsHeartbeat(t *testing.T) {
	broker := NewBroker()
	h := NewHandler(broker, func(*http.Request) string { return "topic" }, WithHeartbeat(5*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	waitForBody(t, rec, ": heartbeat\n\n")
	cancel()
	<-done
}

// nonFlushingWriter wraps an http.ResponseWriter without exposing
// http.Flusher, to exercise Handler's streaming-unsupported path.
type nonFlushingWriter struct {
	http.ResponseWriter
}

// waitForSubscriber blocks until topic has at least one subscriber on b, or
// fails the test after a timeout.
func waitForSubscriber(t *testing.T, b *Broker, topic string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		n := len(b.topics[topic])
		b.mu.RUnlock()
		if n > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for subscriber")
}

// waitForBody blocks until rec's body contains want, or fails the test
// after a timeout.
func waitForBody(t *testing.T, rec *syncRecorder, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(rec.String(), want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for body to contain %q; got %q", want, rec.String())
}
