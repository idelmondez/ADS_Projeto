# Multi-master topology on port 10000

## Context

The repository currently supports a master/worker prototype with heartbeat, election, and master-to-master coordination. The code already accepts `MASTER_HOST` and `MASTER_PORT` to force the master endpoint, and the worker can discover a master via broadcast discovery when no explicit master is configured.

Assume all 15 masters are connected to the same wired LAN, so broadcast discovery can reach every node that should join the coordination mesh.

For this design, assume:
- There are 15 masters connected in the same coordination network.
- All masters use port `10000` for master-to-master connectivity.
- The current node is `Master 3` with IP `10.62.206.207`.

## Goal

Make the topology and configuration explicit so the system can be deployed with 15 masters on port 10000 without ambiguity about the current master identity and address.

## Assumptions

- Each master is identified by a stable `master_id` such as `Master_1` through `Master_15`.
- The current node should be treated as `Master_3` and should use `10.62.206.207:10000` as its master endpoint.
- Existing worker behavior remains unchanged unless a follow-up implementation explicitly needs it.
- No new wire protocol is introduced for this design; the focus is on configuration, naming, and deployment clarity.
- Broadcast discovery is limited to the same broadcast domain and the common coordination port `10000`.

## Proposed Design

### 1. Canonical master identity

Each master process should carry three pieces of information:
- `SERVER_UUID`: stable identity string, for example `Master_3`
- `MASTER_HOST`: bind/addressable IP, for example `10.62.206.207`
- `MASTER_PORT`: coordination port, fixed at `10000`

For the current node:
- `SERVER_UUID = "Master_3"`
- `MASTER_HOST = "10.62.206.207"`
- `MASTER_PORT = 10000`

### 2. Static peer list

Maintain a fixed list of the 15 master endpoints for master-to-master coordination.

Shape:
- `MASTER_1 -> <ip>:10000`
- `MASTER_2 -> <ip>:10000`
- `MASTER_3 -> 10.62.206.207:10000`
- ...
- `MASTER_15 -> <ip>:10000`

This list can live in configuration or environment variables, but the design should keep the port consistent across all masters.

### 3. Configuration precedence

Use this order:
1. Explicit environment variables
2. Repository defaults for local development
3. Auto-detected IP only when no explicit host is provided

For `Master 3`, explicit values should win:
- `MASTER_HOST=10.62.206.207`
- `MASTER_PORT=10000`
- `SERVER_UUID=Master_3`

### 4. Compatibility with existing worker logic

Workers should continue to talk to a specific master endpoint. If a worker is configured to use Master 3, it should point to `10.62.206.207:10000`.

Broadcast discovery and election logic remain usable, but they should resolve to the same fixed master port when a master is found.

## Expected Outcome

- The deployment can be described unambiguously as 15 masters on one coordination port.
- Master 3 is uniquely identified as `Master_3@10.62.206.207:10000`.
- Existing code paths for heartbeat, task handling, and master-to-master messaging remain compatible.

## Non-goals

- No redistribution algorithm for the 15 masters.
- No new election protocol.
- No protocol changes to worker messages.
- No load-balancing redesign.

## Open Questions

- Are the 15 masters on distinct IPs, or do some share hosts with different ports in another environment?
- Should the peer list be stored in a config file instead of code or environment variables?
- Do you want the repository to enforce `MASTER_PORT=10000` for all masters, or only document that as the deployment default?
