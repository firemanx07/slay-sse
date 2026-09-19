package sse

import (
	"sync"
	"testing"
	"time"
)

func TestBrokerPublishDeliversToSubscriber(t *testing.T) {
	b := NewBroker()
	c := b.Subscribe("topic")
	defer b.Unsubscribe("topic", c)

	b.Publish("topic", Event{Data: "hi"})

	select {
	case e := <-c.Events():
		if e.Data != "hi" {
			t.Fatalf("got Data %q, want %q", e.Data, "hi")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestBrokerPublishDoesNotCrossTopics(t *testing.T) {
	b := NewBroker()
	c := b.Subscribe("other")
	defer b.Unsubscribe("other", c)

	b.Publish("topic", Event{Data: "hi"})

	select {
	case e := <-c.Events():
		t.Fatalf("unexpected event on unrelated topic: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBrokerUnsubscribeStopsDelivery(t *testing.T) {
	b := NewBroker()
	c := b.Subscribe("topic")
	b.Unsubscribe("topic", c)

	b.Publish("topic", Event{Data: "hi"})

	select {
	case e := <-c.Events():
		t.Fatalf("unexpected event after unsubscribe: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBrokerDropsWhenClientBufferFull(t *testing.T) {
	var mu sync.Mutex
	var dropped int
	b := NewBroker(WithClientBuffer(1), WithDropFunc(func(topic string, e Event) {
		mu.Lock()
		dropped++
		mu.Unlock()
	}))
	c := b.Subscribe("topic")
	defer b.Unsubscribe("topic", c)

	b.Publish("topic", Event{Data: "1"})
	b.Publish("topic", Event{Data: "2"})
	b.Publish("topic", Event{Data: "3"})

	mu.Lock()
	got := dropped
	mu.Unlock()
	if got == 0 {
		t.Fatal("expected at least one dropped event")
	}
}

func TestBrokerConcurrentPublishSubscribe(t *testing.T) {
	b := NewBroker(WithClientBuffer(64))
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := b.Subscribe("topic")
			defer b.Unsubscribe("topic", c)
			for range 10 {
				select {
				case <-c.Events():
				case <-time.After(time.Second):
				}
			}
		}()
	}
	for range 200 {
		b.Publish("topic", Event{Data: "x"})
	}
	wg.Wait()
}

func TestBrokerSubscribeAndReplayNoStoreReturnsNil(t *testing.T) {
	b := NewBroker()
	c, replay := b.SubscribeAndReplay("topic", "")
	defer b.Unsubscribe("topic", c)
	if replay != nil {
		t.Fatalf("got %+v, want nil", replay)
	}
}

// blockingReplayStore wraps a ReplayStore whose Since call pauses until
// proceed is closed, after signaling sinceHit — used to pin down the
// window during which SubscribeAndReplay must exclude Publish.
type blockingReplayStore struct {
	inner    ReplayStore
	sinceHit chan struct{}
	proceed  chan struct{}
}

func (s *blockingReplayStore) Add(topic string, e Event) { s.inner.Add(topic, e) }

func (s *blockingReplayStore) Since(topic, lastEventID string) []Event {
	close(s.sinceHit)
	<-s.proceed
	return s.inner.Since(topic, lastEventID)
}

func TestBrokerSubscribeAndReplayIsAtomicWithPublish(t *testing.T) {
	store := &blockingReplayStore{
		inner:    NewMemoryReplayStore(10),
		sinceHit: make(chan struct{}),
		proceed:  make(chan struct{}),
	}
	b := NewBroker(WithReplayStore(store))

	var (
		client *Client
		replay []Event
	)
	subscribeDone := make(chan struct{})
	go func() {
		client, replay = b.SubscribeAndReplay("topic", "")
		close(subscribeDone)
	}()

	select {
	case <-store.sinceHit:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Since to be called")
	}

	publishDone := make(chan struct{})
	go func() {
		b.Publish("topic", Event{ID: "1", Data: "hello"})
		close(publishDone)
	}()

	select {
	case <-publishDone:
		t.Fatal("Publish returned before SubscribeAndReplay released the lock")
	case <-time.After(50 * time.Millisecond):
	}

	close(store.proceed)

	select {
	case <-subscribeDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SubscribeAndReplay")
	}
	<-publishDone

	if len(replay) != 0 {
		t.Fatalf("got %d replayed events, want 0 (event was published after the snapshot)", len(replay))
	}

	select {
	case e := <-client.Events():
		if e.ID != "1" {
			t.Fatalf("got event ID %q, want %q", e.ID, "1")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live delivery")
	}

	select {
	case e := <-client.Events():
		t.Fatalf("unexpected second delivery: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}
