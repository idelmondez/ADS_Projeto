import json
import socket
import threading
import time
import unittest

import master


def _get_free_tcp_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class MasterCommListenerTests(unittest.TestCase):
    def test_master_comm_listener_registers_peer_ip(self):
        stop_event = threading.Event()
        port = _get_free_tcp_port()

        thread = threading.Thread(
            target=master.iniciar_master_comm_listener,
            args=(stop_event, "127.0.0.1", port),
            daemon=True,
        )
        thread.start()
        time.sleep(0.1)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(1.0)
        client.connect(("127.0.0.1", port))

        payload = {
            "type": "announce_master",
            "request_id": "1",
            "payload": {"master_address": "127.0.0.1:10000"},
        }
        client.sendall((json.dumps(payload) + "\n").encode())

        data = b""
        while b"\n" not in data:
            chunk = client.recv(4096)
            if not chunk:
                break
            data += chunk

        resp = json.loads(data.split(b"\n", 1)[0].decode().strip())

        self.assertEqual(resp.get("type"), "announce_ack")
        self.assertIn(("127.0.0.1", 10000), master.list_master_peers())

        client.close()
        stop_event.set()


if __name__ == "__main__":
    unittest.main()