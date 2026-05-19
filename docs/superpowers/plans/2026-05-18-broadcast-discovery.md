# Broadcast Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add worker-initiated UDP broadcast discovery on port 2103 before election, reusing HEARTBEAT/ALIVE with no new contracts.

**Architecture:** Master starts a UDP listener that replies to broadcast HEARTBEAT with ALIVE. Worker adds a broadcast discovery helper (3 attempts, 3s timeout) and triggers discovery on startup (non-blocking) and after heartbeat failures before election, without changing TCP flows.

**Tech Stack:** Python 3.13 stdlib (socket, threading, json, unittest)

---

## File Structure
- Create: tests/test_broadcast_discovery.py
  - Master UDP listener test and worker discovery test (unittest).
- Modify: master.py:11-56, 87-120, 390-405
  - Add discovery port constant, UDP listener, and wire it into startup.
- Modify: worker.py:11-80, 82-170, 414-429
  - Add discovery constants, broadcast helper, async trigger, and main loop integration.

---

### Task 1: Master UDP Discovery Listener (TDD)

**Files:**
- Create: tests/test_broadcast_discovery.py
- Modify: master.py:11-56, 87-120, 390-405
- Test: tests/test_broadcast_discovery.py

- [ ] **Step 1: Write the failing test for the master UDP listener**

```python
import json
import socket
import threading
import time
import unittest

import master


def _get_free_udp_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class BroadcastDiscoveryTests(unittest.TestCase):
    def test_master_udp_listener_replies_alive(self):
        stop_event = threading.Event()
        port = _get_free_udp_port()

        thread = threading.Thread(
            target=master.udp_discovery_listener,
            args=(stop_event, "127.0.0.1", port),
            daemon=True,
        )
        thread.start()
        time.sleep(0.1)

        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.settimeout(1.0)
        payload = {"SERVER_UUID": master.SERVER_UUID, "TASK": "HEARTBEAT"}
        client.sendto((json.dumps(payload) + "\n").encode(), ("127.0.0.1", port))
        data, _ = client.recvfrom(4096)
        resp = json.loads(data.decode().strip())

        self.assertEqual(resp.get("TASK"), "HEARTBEAT")
        self.assertEqual(resp.get("RESPONSE"), "ALIVE")

        client.close()
        stop_event.set()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
a. c:/Users/ivand/Downloads/ADS_Projeto-main/.venv/Scripts/python.exe -m unittest tests/test_broadcast_discovery.py -v
```
Expected: FAIL with `AttributeError: module 'master' has no attribute 'udp_discovery_listener'`.

- [ ] **Step 3: Implement the UDP discovery listener in master.py**

Add a constant near the top:
```python
DISCOVERY_PORT = 2103
```

Add the UDP listener function after `send_json_line`:
```python
def udp_discovery_listener(stop_event, host="0.0.0.0", port=DISCOVERY_PORT):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1.0)
    except Exception as e:
        log(f"Erro ao iniciar listener UDP de discovery: {e}")
        return

    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            continue
        except Exception:
            continue

        try:
            payload = json.loads(data.decode().strip())
        except Exception:
            continue

        if payload.get("TASK") != "HEARTBEAT":
            continue

        response = {
            "SERVER_UUID": SERVER_UUID,
            "TASK": "HEARTBEAT",
            "RESPONSE": "ALIVE",
        }
        try:
            sock.sendto((json.dumps(response) + "\n").encode(), addr)
        except Exception:
            continue

    try:
        sock.close()
    except Exception:
        pass
```

Wire the listener into `iniciar_master`:
```python
    threading.Thread(target=monitor_borrowed_workers_loop, daemon=True).start()
    threading.Thread(target=udp_discovery_listener, args=(stop_event,), daemon=True).start()
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
c:/Users/ivand/Downloads/ADS_Projeto-main/.venv/Scripts/python.exe -m unittest tests/test_broadcast_discovery.py -v
```
Expected: PASS with `OK (1 test)`.

- [ ] **Step 5: Commit**

```bash
git add tests/test_broadcast_discovery.py master.py
git commit -m "feat: add master UDP discovery listener"
```

---

### Task 2: Worker Broadcast Discovery (TDD)

**Files:**
- Modify: tests/test_broadcast_discovery.py
- Modify: worker.py:11-80, 82-170, 414-429
- Test: tests/test_broadcast_discovery.py

- [ ] **Step 1: Add a failing worker discovery test**

Append to tests/test_broadcast_discovery.py:
```python
import worker


class BroadcastDiscoveryTests(unittest.TestCase):
    def test_worker_discovery_returns_master(self):
        stop_event = threading.Event()
        port = _get_free_udp_port()

        thread = threading.Thread(
            target=master.udp_discovery_listener,
            args=(stop_event, "127.0.0.1", port),
            daemon=True,
        )
        thread.start()
        time.sleep(0.1)

        result = worker.discover_master_via_broadcast(
            broadcast_ip="127.0.0.1",
            port=port,
            attempts=1,
            timeout=0.5,
            master_port=2003,
        )
        self.assertEqual(result, ("127.0.0.1", 2003))
        stop_event.set()
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
c:/Users/ivand/Downloads/ADS_Projeto-main/.venv/Scripts/python.exe -m unittest tests/test_broadcast_discovery.py -v
```
Expected: FAIL with `AttributeError: module 'worker' has no attribute 'discover_master_via_broadcast'`.

- [ ] **Step 3: Implement worker broadcast discovery and integrate into main_loop**

Add constants and discovery state near the top:
```python
DISCOVERY_PORT = 2103
DISCOVERY_ATTEMPTS = 3
DISCOVERY_TIMEOUT = 3
DISCOVERY_BROADCAST_IP = "255.255.255.255"
MASTER_FROM_ENV = False

discovery_thread = None
discovery_result = None
discovery_lock = threading.Lock()
```

Set `MASTER_FROM_ENV = True` when `MASTER_HOST`/`MASTER_PORT` are provided.

Add the discovery helper after `send_and_receive_json`:
```python
def discover_master_via_broadcast(
    broadcast_ip=DISCOVERY_BROADCAST_IP,
    port=DISCOVERY_PORT,
    attempts=DISCOVERY_ATTEMPTS,
    timeout=DISCOVERY_TIMEOUT,
    master_port=None,
):
    if master_port is None:
        with state_lock:
            master_port = MASTER[1]

    payload = {"SERVER_UUID": MASTER_UUID, "TASK": "HEARTBEAT"}
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.settimeout(timeout)
        for _ in range(attempts):
            try:
                s.sendto((json.dumps(payload) + "\n").encode(), (broadcast_ip, port))
                data, addr = s.recvfrom(4096)
            except socket.timeout:
                continue
            except Exception as e:
                log(f"Erro no broadcast discovery: {e}")
                continue

            try:
                resp = json.loads(data.decode().strip())
            except Exception:
                continue

            if resp.get("TASK") == "HEARTBEAT" and resp.get("RESPONSE") == "ALIVE":
                return (addr[0], master_port)

    return None
```

Add async discovery trigger helpers:
```python
def _run_discovery():
    global discovery_result
    result = discover_master_via_broadcast()
    if result:
        with discovery_lock:
            discovery_result = result


def start_discovery_thread():
    global discovery_thread
    with discovery_lock:
        if discovery_thread and discovery_thread.is_alive():
            return
        discovery_thread = threading.Thread(target=_run_discovery, daemon=True)
        discovery_thread.start()


def apply_discovery_if_found():
    global falhas, eleicao_em_andamento, discovery_result, MASTER
    with discovery_lock:
        result = discovery_result
        discovery_result = None
    if result:
        with state_lock:
            MASTER = result
        falhas = 0
        eleicao_em_andamento = False
        log(f"Master descoberto via broadcast: {MASTER}")
        return True
    return False
```

Update `main_loop` to trigger discovery without blocking and use it before election:
```python
    registrar_legacy()
    if not MASTER_FROM_ENV:
        start_discovery_thread()

    while True:
        ok = heartbeat()

        if not ok:
            start_discovery_thread()

        if not ok and falhas >= 4 and not eleicao_em_andamento:
            if apply_discovery_if_found():
                time.sleep(3)
                continue
            eleicao_em_andamento = True
            eleicao()

        if ok:
            solicitar_tarefa_e_processar()

        time.sleep(3)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
c:/Users/ivand/Downloads/ADS_Projeto-main/.venv/Scripts/python.exe -m unittest tests/test_broadcast_discovery.py -v
```
Expected: PASS with `OK (2 tests)`.

- [ ] **Step 5: Commit**

```bash
git add tests/test_broadcast_discovery.py worker.py
git commit -m "feat: add worker broadcast discovery"
```

---

## Self-Review
- **Spec coverage:**
  - Worker-initiated UDP broadcast on port 2103 before election: Task 2.
  - 3 attempts with 3s timeout: Task 2 constants.
  - Master UDP listener replying ALIVE: Task 1.
  - No new contracts: no new payload fields or message types in any task.
- **Placeholder scan:** No TODO/TBD markers used.
- **Type consistency:** `discover_master_via_broadcast` returns `(ip, port)` consistently; main loop applies result to `MASTER`.

---

## Execution Handoff
Plan complete and saved to docs/superpowers/plans/2026-05-18-broadcast-discovery.md. Two execution options:

1. Subagent-Driven (recommended) - I dispatch a fresh subagent per task, review between tasks, fast iteration
2. Inline Execution - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
