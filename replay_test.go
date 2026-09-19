package sse

import "testing"

// TestMemoryReplayStoreSinceEmptyLastID verifies an empty lastEventID
// returns every recorded event for the topic.
func TestMemoryReplayStoreSinceEmptyLastID(t *testing.T) {
	s := NewMemoryReplayStore(10)
	s.Add("topic", Event{ID: "1", Data: "a"})
	s.Add("topic", Event{ID: "2", Data: "b"})

	got := s.Since("topic", "")
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
}

// TestMemoryReplayStoreSinceLastID verifies Since returns only the events
// recorded after the given lastEventID.
func TestMemoryReplayStoreSinceLastID(t *testing.T) {
	s := NewMemoryReplayStore(10)
	s.Add("topic", Event{ID: "1", Data: "a"})
	s.Add("topic", Event{ID: "2", Data: "b"})
	s.Add("topic", Event{ID: "3", Data: "c"})

	got := s.Since("topic", "2")
	if len(got) != 1 || got[0].ID != "3" {
		t.Fatalf("got %+v, want a single event with ID 3", got)
	}
}

// TestMemoryReplayStoreEvictsBeyondCapacity verifies the oldest events are
// evicted once a topic's recorded events exceed its capacity.
func TestMemoryReplayStoreEvictsBeyondCapacity(t *testing.T) {
	s := NewMemoryReplayStore(2)
	s.Add("topic", Event{ID: "1"})
	s.Add("topic", Event{ID: "2"})
	s.Add("topic", Event{ID: "3"})

	got := s.Since("topic", "")
	if len(got) != 2 || got[0].ID != "2" || got[1].ID != "3" {
		t.Fatalf("got %+v, want ids [2 3]", got)
	}
}

// TestMemoryReplayStoreUnknownLastIDReturnsAll verifies an unknown or
// evicted lastEventID returns every retained event, per the ReplayStore
// interface's documented contract.
func TestMemoryReplayStoreUnknownLastIDReturnsAll(t *testing.T) {
	s := NewMemoryReplayStore(10)
	s.Add("topic", Event{ID: "1"})
	s.Add("topic", Event{ID: "2"})

	got := s.Since("topic", "does-not-exist")
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2 (unknown/evicted id returns everything retained)", len(got))
	}
}

// TestNewMemoryReplayStoreClampsNegativeCapacity verifies a negative
// capacity is treated as zero rather than panicking on the first Add.
func TestNewMemoryReplayStoreClampsNegativeCapacity(t *testing.T) {
	s := NewMemoryReplayStore(-1)
	s.Add("topic", Event{ID: "1"})
	s.Add("topic", Event{ID: "2"})

	got := s.Since("topic", "")
	if len(got) != 0 {
		t.Fatalf("got %d events, want 0 (negative capacity clamped to zero)", len(got))
	}
}

// TestMemoryReplayStoreUnknownTopicReturnsEmpty verifies Since returns no
// events for a topic that was never recorded.
func TestMemoryReplayStoreUnknownTopicReturnsEmpty(t *testing.T) {
	s := NewMemoryReplayStore(10)
	got := s.Since("missing", "")
	if len(got) != 0 {
		t.Fatalf("got %d events, want 0", len(got))
	}
}
