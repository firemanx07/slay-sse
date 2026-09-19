# Changelog

All notable changes to this project are documented here.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.1.0] - 2026-09-19

### Added

- Initial release: `Event`, `Client`, `Broker`, `Handler`, `ReplayStore`/`MemoryReplayStore` — a
  transport-only, dependency-free Server-Sent Events module. Topic-based publish/subscribe,
  non-blocking per-client backpressure, Last-Event-ID reconnection with atomic
  `SubscribeAndReplay`, and configurable heartbeats.
- `examples/tokenstream`: a runnable HTTP server demonstrating the API with a simulated token
  stream.
- CI (build/vet/gofmt/test -race/lint/vulncheck), `codecov.yml` coverage gate, and OSS
  contribution scaffolding (issue templates, PR template, dependabot, `FUNDING.yml`).
