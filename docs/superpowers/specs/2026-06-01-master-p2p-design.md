# Master P2P Design

Date: 2026-06-01

## Context
This design implements Sprint 03 requirements: a Master-to-Master P2P negotiation layer enabling a saturated Master to request and receive temporary Workers from a neighbor Master. It complements existing legacy Master/Worker TCP flows and the broadcast discovery added in previous sprints.

## Goals
- Provide a robust, concurrent Master↔Master protocol over TCP (coordination port default: 10000).
- Support request_help → response_(accepted|rejected) → command_redirect → register_temporary_worker → command_release → notify_worker_returned flow.
- Ensure interoperability with existing legacy Worker protocol (heartbeat, QUERY/NO_TASK, STATUS/ACK).
- Testable via unit and integration tests that simulate Masters and Workers.

## Non-goals
- Redesign election or worker scheduling algorithms. Keep legacy behavior unchanged except for negotiated redirects.

## High-level Architecture
- Each Master process provides:
  - TCP server listening on `MASTER_PORT` (default 10000) that accepts connections from Workers and Masters.
  - UDP discovery responder (reuse existing 2103 listener for workers; masters use broadcast on 10000 for peer discovery).
  - Connection pool / re-use for outbound Master-to-Master connections.
  - Thread-based concurrency with fine-grained locks for shared state.

## Key Data Structures
- `task_queue` (queue.Queue)
- `worker_registry` (dict) — entries track borrowed/local, control_address, busy, last_seen
- `borrowed_workers` (dict) — worker_id -> original_master_address
- `master_peers` (set) — known master endpoints
- Locks for each structure to avoid races.

## Wire Protocol (JSON + newline delim)
- Legacy Worker ↔ Master: unchanged (HEARTBEAT/ALIVE, WORKER ALIVE presentation, QUERY/NO_TASK, STATUS/ACK).
- Master ↔ Master: messages with fields `{ "type": "...", "request_id": "uuid4", "payload": {...} }`.

Core message types and payloads (summary):
- `request_help` — payload: `{master_id, current_load, capacity, workers_needed}`
- `response_accepted` — payload: `{workers_offered, worker_details: [{id, address}]}`
- `response_rejected` — payload: `{reason}`
- `command_redirect` — payload to Worker: `{new_master_address}`
- `register_temporary_worker` — payload from Worker to new Master: `{worker_id, original_master_address}`
- `command_release` — payload to Worker: `{original_master_address}`
- `notify_worker_returned` — payload to original Master: `{worker_id}`

All Master-to-Master requests must reuse the same `request_id` in the corresponding reply.

## Flows (concise)
- Requesting help:
  1. Master A checks saturation (current_load > CAPACITY).
  2. Master A selects candidate peers from `master_peers` and opens a TCP connection (or reuses pooled one).
  3. Master A sends `request_help` with UUID v4 and waits up to 5s for a response.
  4. On `response_accepted`, Master A schedules `command_redirect` for each offered worker.

- Redirecting Workers:
  1. Master B sends `command_redirect` over the Worker’s control TCP connection (existing connection used when possible).
  2. Worker closes gracefully, connects to Master A, and immediately sends `register_temporary_worker`.
  3. Master A registers worker as borrowed and serves tasks; borrowed_workers updated.

- Returning Workers:
  1. When Master A load < RELEASE_THRESHOLD, it sends `command_release` to worker and `notify_worker_returned` to Master B.
  2. Worker reconnects to original Master and Master B removes borrowed state.

## Concurrency & Robustness
- Use threads per accepted connection and a small thread pool for outgoing coordination tasks.
- Protect mutable structures with locks; keep critical sections short.
- Use timeouts on network operations (5s default for master replies, 3s for UDP discovery round-trips).
- Make handlers idempotent and tolerant of duplicate messages (e.g., re-registering the same worker is a no-op).
- Log `request_id`, `type`, timestamps for traceability.

## Error Handling
- If `request_help` times out, mark peer as slow/unavailable and try next peer.
- If `command_redirect` fails for a specific Worker, cancel remaining redirects for that request and notify original requester with a failure log.
- On partial failures, perform best-effort cleanup (reverse any partial state) and log details.

## Testing Plan
- Unit tests for `handle_type_message` semantics and request_id correlation.
- Integration tests simulating two Masters and N Workers to validate CT01–CT06 (request accepted, rejected, register_temporary_worker, task execution, release).
- Add `tests/test_master_p2p.py` for protocol scenarios; tests must run on localhost using ephemeral ports.

## Files to add (implementation stage)
- `master_p2p.py` — standalone Master implementation (can be used as drop-in via env var).
- `worker_p2p.py` — Worker with `command_redirect`/`command_release` handlers and `register_temporary_worker` logic.
- `tests/test_master_p2p.py` — unit + integration tests.
- `docs/superpowers/specs/2026-06-01-master-p2p-design.md` — (this file).

## Compatibility & Deployment
- Default `MASTER_PORT=10000`. `MASTER_HOST`/`MASTER_PORT` env vars override detection.
- Keep `master.py`/`worker.py` intact during initial rollout. `master_p2p.py` and `worker_p2p.py` will use the same legacy handlers where appropriate to interoperate.

## Next steps (after your approval)
1. Create `master_p2p.py` and `worker_p2p.py` with threads, handlers, and connection pool.
2. Add TDD tests in `tests/test_master_p2p.py` and run them locally.
3. Iterate on failure cases and merge to `main` once tests pass.

Please review this design. If it looks good, reply `approve` and I will generate the implementation and tests. If you want changes, tell me which section to adjust.
