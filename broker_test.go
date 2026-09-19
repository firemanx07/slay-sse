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

func TestBrokerReplayNoStoreReturnsNil(t *testing.T) {
	b := NewBroker()
	if got := b.Replay("topic", ""); got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}
