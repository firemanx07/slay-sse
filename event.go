package sse

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// Event is a single Server-Sent Event.
type Event struct {
	// ID sets the event's id field, used by clients for Last-Event-ID
	// reconnection. Empty means no id line is written.
	ID string
	// Name sets the event's event field. Empty means the client sees it as
	// the default "message" event.
	Name string
	// Data is the event payload. It is split on newlines and each line is
	// written as its own data field, per the SSE wire format.
	Data string
	// Retry sets the client's reconnection delay via the retry field. Zero
	// means no retry line is written.
	Retry time.Duration
}

// encode writes e to w in the SSE wire format, terminated by a blank line.
func (e Event) encode(w io.Writer) error {
	var b strings.Builder
	if e.ID != "" {
		fmt.Fprintf(&b, "id: %s\n", sanitizeField(e.ID))
	}
	if e.Name != "" {
		fmt.Fprintf(&b, "event: %s\n", sanitizeField(e.Name))
	}
	if e.Retry > 0 {
		fmt.Fprintf(&b, "retry: %d\n", e.Retry.Milliseconds())
	}
	data := strings.ReplaceAll(e.Data, "\r\n", "\n")
	for _, line := range strings.Split(data, "\n") {
		fmt.Fprintf(&b, "data: %s\n", line)
	}
	b.WriteByte('\n')
	_, err := io.WriteString(w, b.String())
	return err
}

// sanitizeField collapses newlines in a single-line SSE field (id, event).
func sanitizeField(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.ReplaceAll(s, "\r", " ")
}
