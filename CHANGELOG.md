# Changelog

## 2026-05-19 — Broadcast discovery (merged)

- Merged branch `broadcast-discovery` into `main`.
- Implemented UDP broadcast discovery:
  - `udp_discovery_listener` added in `master.py` (listens on UDP port 2103 and replies to legacy HEARTBEAT with RESPONSE=ALIVE).
  - `discover_master_via_broadcast` and helpers added in `worker.py` (worker-initiated discovery; default 3 attempts × 3s timeout).
- Added unit tests: `tests/test_broadcast_discovery.py` (2 tests passing).
- Added design spec and implementation plan under `docs/superpowers/`.

Notes:
- Reuses existing HEARTBEAT/ALIVE contract; no new message fields introduced.
- Merged to `main` and branch/worktree cleaned up.
