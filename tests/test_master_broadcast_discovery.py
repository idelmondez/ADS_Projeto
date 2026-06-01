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


class MasterBroadcastDiscoveryTests(unittest.TestCase):
    def test_master_broadcast_discovers_and_registers_peer(self):
        stop_event = threading.Event()
        port = _get_free_udp_port()

        original_host = master.HOST
        original_comm_port = master.MASTER_COMM_PORT

        try:
            master.HOST = "127.0.0.1"
            master.MASTER_COMM_PORT = port

            thread = threading.Thread(
                target=master.udp_master_discovery_listener,
                args=(stop_event, "127.0.0.1", port),
                daemon=True,
            )
            thread.start()
            time.sleep(0.1)

            peers = master.discover_master_peers_via_broadcast(
                broadcast_ip="127.0.0.1",
                port=port,
                attempts=1,
                timeout=0.5,
            )

            self.assertIn(("127.0.0.1", port), peers)
        finally:
            master.HOST = original_host
            master.MASTER_COMM_PORT = original_comm_port
            stop_event.set()


if __name__ == "__main__":
    unittest.main()