package sse

import (
	"strings"
	"testing"
	"time"
)

// TestEventEncode verifies the SSE wire-format output for id/name/data/retry
// combinations, including line-ending normalization and field sanitization.
func TestEventEncode(t *testing.T) {
	tests := []struct {
		name string
		e    Event
		want string
	}{
		{
			name: "data only",
			e:    Event{Data: "hello"},
			want: "data: hello\n\n",
		},
		{
			name: "id and name",
			e:    Event{ID: "1", Name: "chunk", Data: "hello"},
			want: "id: 1\nevent: chunk\ndata: hello\n\n",
		},
		{
			name: "multiline data",
			e:    Event{Data: "line1\nline2"},
			want: "data: line1\ndata: line2\n\n",
		},
		{
			name: "crlf data is normalized",
			e:    Event{Data: "line1\r\nline2"},
			want: "data: line1\ndata: line2\n\n",
		},
		{
			name: "lone cr data is normalized",
			e:    Event{Data: "line1\rline2"},
			want: "data: line1\ndata: line2\n\n",
		},
		{
			name: "mixed line endings are normalized",
			e:    Event{Data: "a\r\nb\rc\nd"},
			want: "data: a\ndata: b\ndata: c\ndata: d\n\n",
		},
		{
			name: "retry",
			e:    Event{Data: "x", Retry: 2500 * time.Millisecond},
			want: "retry: 2500\ndata: x\n\n",
		},
		{
			name: "id with embedded newline is sanitized",
			e:    Event{ID: "a\nb", Data: "x"},
			want: "id: a b\ndata: x\n\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b strings.Builder
			if err := tt.e.encode(&b); err != nil {
				t.Fatalf("encode: %v", err)
			}
			if got := b.String(); got != tt.want {
				t.Errorf("encode() = %q, want %q", got, tt.want)
			}
		})
	}
}
