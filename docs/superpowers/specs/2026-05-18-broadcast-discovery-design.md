# Broadcast Discovery Design (Worker Initiated)

Date: 2026-05-18

## Context
Workers must connect to a master when instantiated. If no master is reachable, the worker should search for a master via broadcast. If none is found, the existing election flow must proceed. The solution must not introduce new contracts and must avoid regressions.

## Goals
- Allow workers to discover a master via UDP broadcast before election.
- Reuse the existing HEARTBEAT/ALIVE contract without adding fields.
- Keep TCP behavior and task flow unchanged.
- Preserve existing env var behavior and defaults.

## Non-Goals
- Changing the election algorithm or its selection criteria.
- Adding new message types or payload fields.
- Adding new environment variables or config files.

## Current Behavior (Summary)
- Worker uses MASTER_HOST/MASTER_PORT if defined; otherwise it assumes MEU_IP:2003.
- On heartbeat failure, the worker proceeds to election.
- Master handles HEARTBEAT over TCP only.

## Proposed Behavior
### Worker Discovery Flow
- Broadcast discovery uses UDP to port 2103.
- Broadcast payload is the existing heartbeat message:
  - {"SERVER_UUID": "Master_A", "TASK": "HEARTBEAT"}
- The worker performs 3 attempts, each with a 3s timeout.
- On the first valid ALIVE response, the worker sets MASTER to (reply_ip, MASTER_PORT or 2003), resets failure counters, and resumes normal flow.
- If no response is received after 3 attempts, the worker proceeds with the existing election flow.

### When Discovery Runs
- Startup:
  - If MASTER_HOST/MASTER_PORT is set, the worker uses it directly and skips initial discovery.
  - If not set, the worker runs discovery before election.
- After heartbeat failures:
  - Before starting election, the worker runs discovery (3 attempts, 3s each).

### Master UDP Listener
- Master starts a UDP listener on 0.0.0.0:2103 in a background thread.
- On receiving a valid HEARTBEAT payload, it responds with the existing ALIVE payload to the sender address.
- Invalid JSON or unrelated payloads are ignored without interrupting the main TCP server.

## Error Handling and Robustness
- UDP bind failures are logged; master continues running with TCP only.
- Worker ignores invalid or empty UDP responses and continues attempts.
- Broadcast attempts do not change existing TCP error handling.

## Testing
- Manual:
  1) Start master, start worker without MASTER_HOST; worker should discover master via UDP broadcast.
  2) Stop master; worker should attempt broadcast (3 x 3s) before election.
- Regression:
  - Existing tests should continue to pass without changes to their expected behavior.

## Compatibility
- No new contracts or message types.
- No changes to existing TCP payloads.
- No new environment variables.
