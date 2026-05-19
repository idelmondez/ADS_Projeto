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
