package sse

import "sync"

// ReplayStore records published events so reconnecting clients can catch
// up via Last-Event-ID.
type ReplayStore interface {
	// Add records e as having been published on topic.
	Add(topic string, e Event)
	// Since returns the events recorded for topic after lastEventID, oldest
	// first. An empty lastEventID, or one not found, returns every
	// recorded event for topic.
	Since(topic, lastEventID string) []Event
}

// MemoryReplayStore is a ReplayStore backed by a fixed-size, in-memory
// ring buffer per topic. It does not persist across process restarts.
type MemoryReplayStore struct {
	mu       sync.Mutex
	capacity int
	topics   map[string][]Event
}

// NewMemoryReplayStore creates a MemoryReplayStore retaining up to
// capacity events per topic.
func NewMemoryReplayStore(capacity int) *MemoryReplayStore {
	return &MemoryReplayStore{
		capacity: capacity,
		topics:   make(map[string][]Event),
	}
}

// Add implements ReplayStore.
func (s *MemoryReplayStore) Add(topic string, e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[topic] = append(s.topics[topic], e)
	if buf := s.topics[topic]; len(buf) > s.capacity {
		s.topics[topic] = buf[len(buf)-s.capacity:]
	}
}

// Since implements ReplayStore.
func (s *MemoryReplayStore) Since(topic, lastEventID string) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := s.topics[topic]
	start := 0
	if lastEventID != "" {
		start = len(buf)
		for i, e := range buf {
			if e.ID == lastEventID {
				start = i + 1
				break
			}
		}
	}
	out := make([]Event, len(buf[start:]))
	copy(out, buf[start:])
	return out
}
