import socket
import threading
import json
import time


class WorkerNet:
    def __init__(self, worker_id, control_host="127.0.0.1", control_port=0):
        self.worker_id = worker_id
        self.control_host = control_host
        self.control_port = int(control_port)
        self.server = None
        self.running = False
        self.pending_new_master = None

    def start(self):
        # start control server to receive command_redirect/command_release
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.control_host, self.control_port))
        self.control_port = self.server.getsockname()[1]
        self.server.listen(1)
        self.running = True
        threading.Thread(target=self._serve, daemon=True).start()

    def stop(self):
        self.running = False
        try:
            if self.server:
                self.server.close()
        except Exception:
            pass

    def _serve(self):
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
            if msg.get("type") == "command_redirect":
                new_master = msg.get("payload", {}).get("new_master_address")
                # simulate connecting to new master and registering
                self.pending_new_master = new_master
                # respond ack
                self._send_json_line(conn, {"type": "redirect_ack", "request_id": msg.get("request_id"), "payload": {"status": "ACK"}})
            elif msg.get("type") == "command_release":
                # respond ack
                self._send_json_line(conn, {"type": "release_ack", "request_id": msg.get("request_id"), "payload": {"status": "ACK"}})
            else:
                self._send_json_line(conn, {"type": "error", "request_id": msg.get("request_id"), "payload": {"reason": "unknown"}})
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def control_address(self):
        return (self.control_host, self.control_port)
