import socket
import threading
import json
import uuid
import time


class MasterNet:
    def __init__(self, master_id, host="127.0.0.1", port=10000):
        self.master_id = master_id
        self.host = host
        self.port = int(port)
        self.worker_registry = {}  # worker_id -> (host, port)
        self.borrowed_workers = {}  # worker_id -> original_master_address
        self.server = None
        self.lock = threading.Lock()
        self.running = False

    def start(self):
        self.running = True
        t = threading.Thread(target=self._serve, daemon=True)
        t.start()

    def stop(self):
        self.running = False
        try:
            if self.server:
                self.server.close()
        except Exception:
            pass

    def _serve(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(5)
        while self.running:
            try:
                conn, addr = self.server.accept()
                threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()
            except Exception:
                time.sleep(0.01)

    def _recv_json_line(self, conn):
        data = b""
        while b"\n" not in data:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        if not data:
            return None
        line = data.split(b"\n", 1)[0].decode().strip()
        if not line:
            return None
        return json.loads(line)

    def _send_json_line(self, conn, payload):
        conn.sendall((json.dumps(payload) + "\n").encode())

    def _handle_conn(self, conn, addr):
        try:
            msg = self._recv_json_line(conn)
            if not msg:
                return
            mtype = msg.get("type")
            if mtype == "request_help":
                resp = self._handle_request_help(msg)
                self._send_json_line(conn, resp)
            elif mtype == "register_temporary_worker":
                payload = msg.get("payload", {})
                wid = payload.get("worker_id")
                orig = payload.get("original_master_address")
                with self.lock:
                    self.worker_registry[wid] = (addr[0], None)
                    self.borrowed_workers[wid] = orig
                self._send_json_line(conn, {"type": "register_ack", "request_id": msg.get("request_id"), "payload": {"status": "ACK"}})
            elif mtype == "notify_worker_returned":
                wid = msg.get("payload", {}).get("worker_id")
                with self.lock:
                    self.borrowed_workers.pop(wid, None)
                self._send_json_line(conn, {"type": "notify_ack", "request_id": msg.get("request_id"), "payload": {"status": "ACK"}})
            else:
                self._send_json_line(conn, {"type": "error", "request_id": msg.get("request_id"), "payload": {"reason": "unknown_type"}})
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _handle_request_help(self, msg):
        payload = msg.get("payload", {})
        workers_needed = int(payload.get("workers_needed", 0))
        with self.lock:
            idle = [wid for wid in self.worker_registry.keys()]
        if not idle:
            return {"type": "response_rejected", "request_id": msg.get("request_id"), "payload": {"reason": "no_workers_available"}}
        offered = idle[:workers_needed]
        details = [{"id": w, "address": ":"} for w in offered]

        # send redirects to workers (connect to worker control port if known)
        for wid in offered:
            worker_addr = self.worker_registry.get(wid)
            if worker_addr:
                # attempt to connect to worker control (host, port stored as None if unknown)
                host, port = worker_addr[0], worker_addr[1]
                try:
                    # If worker control port unknown, skip network redirect; assume worker will reconnect
                    if port:
                        with socket.create_connection((host, port), timeout=2) as wc:
                            cmd = {"type": "command_redirect", "request_id": msg.get("request_id"), "payload": {"new_master_address": payload.get("master_id")}}
                            wc.sendall((json.dumps(cmd) + "\n").encode())
                except Exception:
                    pass
        return {"type": "response_accepted", "request_id": msg.get("request_id"), "payload": {"workers_offered": len(details), "worker_details": details}}

    def send_request_help(self, peer_host, peer_port, workers_needed=1):
        req = {"type": "request_help", "request_id": str(uuid.uuid4()), "payload": {"master_id": f"{self.host}:{self.port}", "current_load": 0, "capacity": 100, "workers_needed": workers_needed}}
        try:
            with socket.create_connection((peer_host, peer_port), timeout=5) as s:
                s.sendall((json.dumps(req) + "\n").encode())
                data = b""
                while b"\n" not in data:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                if not data:
                    return None
                resp = json.loads(data.split(b"\n", 1)[0].decode())
                return resp
        except Exception:
            return None
