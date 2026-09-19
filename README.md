# slay-sse

[![Sponsor](https://img.shields.io/badge/sponsor-GitHub%20Sponsors-ea4aaa)](https://github.com/sponsors/firemanx07)
[![CI](https://github.com/firemanx07/slay-sse/actions/workflows/ci.yml/badge.svg)](https://github.com/firemanx07/slay-sse/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/firemanx07/slay-sse/graph/badge.svg)](https://codecov.io/github/firemanx07/slay-sse)

Server-Sent Events for Go: a topic-based broker and an `http.Handler` that streams events to
subscribed clients. Transport-only — the module has no knowledge of what's being streamed (chat
tokens, notifications, or anything else); that framing belongs in the caller.

## Install

```bash
go get github.com/firemanx07/slay-sse
```

## Usage

```go
broker := sse.NewBroker(sse.WithReplayStore(sse.NewMemoryReplayStore(50)))
handler := sse.NewHandler(broker, func(r *http.Request) string {
    return r.PathValue("id") // one topic per chat/session id
})

mux := http.NewServeMux()
mux.Handle("/stream/{id}", handler)

// From anywhere else in the app:
broker.Publish("session-123", sse.Event{ID: "1", Name: "token", Data: "hello"})
```

Run the fuller example (an HTTP server publishing a simulated token stream):

```bash
go run ./examples/tokenstream
curl -N http://localhost:8090/stream
```

## API overview

- `Event{ID, Name, Data, Retry}` — one SSE message.
- `Broker` — `Subscribe`/`Unsubscribe`/`Publish` by topic. Options: `WithClientBuffer`,
  `WithReplayStore`, `WithDropFunc`.
- `Handler` — `http.Handler` that subscribes a request to a topic, replays missed events via
  `Last-Event-ID` when a `ReplayStore` is configured, sends heartbeats, and unsubscribes when the
  client disconnects. Option: `WithHeartbeat`.
- `ReplayStore` — interface for Last-Event-ID catch-up; `MemoryReplayStore` is the built-in,
  in-process, non-persistent implementation.

A client buffer that fills (a slow reader) drops that event for that client rather than blocking
`Publish` for every other subscriber; see `WithDropFunc` to observe drops.

## License

[Apache License 2.0](LICENSE)
