// Command tokenstream runs a minimal HTTP server that publishes simulated
// LLM token events over SSE, demonstrating how sse.Broker and sse.Handler
// are wired together.
package main

import (
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	sse "github.com/firemanx07/slay-sse"
)

const topic = "chat"

// main starts the demo HTTP server and the token-publishing goroutine.
func main() {
	broker := sse.NewBroker(sse.WithReplayStore(sse.NewMemoryReplayStore(50)))
	handler := sse.NewHandler(broker, func(*http.Request) string { return topic })

	mux := http.NewServeMux()
	mux.Handle("/stream", handler)

	go publishTokens(broker)

	srv := &http.Server{Addr: ":8090", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Println("listening on :8090 — curl -N http://localhost:8090/stream")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// publishTokens repeatedly publishes a fixed sentence to topic, one word at
// a time, to simulate an LLM's token stream.
func publishTokens(broker *sse.Broker) {
	tokens := strings.Fields("The quick brown fox jumps over the lazy dog")
	var id int
	for {
		for _, tok := range tokens {
			id++
			broker.Publish(topic, sse.Event{ID: strconv.Itoa(id), Name: "token", Data: tok})
			time.Sleep(200 * time.Millisecond)
		}
		id++
		broker.Publish(topic, sse.Event{ID: strconv.Itoa(id), Name: "done"})
		time.Sleep(2 * time.Second)
	}
}
