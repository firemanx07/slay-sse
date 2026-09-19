package sse

import "testing"

func TestMemoryReplayStoreSinceEmptyLastID(t *testing.T) {
	s := NewMemoryReplayStore(10)
	s.Add("topic", Event{ID: "1", Data: "a"})
	s.Add("topic", Event{ID: "2", Data: "b"})

	got := s.Since("topic", "")
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
}

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

func TestMemoryReplayStoreUnknownLastIDReturnsNoneAfter(t *testing.T) {
	s := NewMemoryReplayStore(10)
	s.Add("topic", Event{ID: "1"})

	got := s.Since("topic", "does-not-exist")
	if len(got) != 0 {
		t.Fatalf("got %d events, want 0", len(got))
	}
}

func TestMemoryReplayStoreUnknownTopicReturnsEmpty(t *testing.T) {
	s := NewMemoryReplayStore(10)
	got := s.Since("missing", "")
	if len(got) != 0 {
		t.Fatalf("got %d events, want 0", len(got))
	}
}
