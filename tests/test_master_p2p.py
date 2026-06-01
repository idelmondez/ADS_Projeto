import threading
import time
import unittest

from master_p2p import MasterP2P
from worker_p2p import WorkerP2P


class MasterP2PTests(unittest.TestCase):
    def test_request_help_and_redirect(self):
        master_a = MasterP2P("Master_A", host="127.0.0.1", port=10000)
        master_b = MasterP2P("Master_B", host="127.0.0.1", port=10001)

        # register peers in-memory
        master_a.add_peer(master_b)
        master_b.add_peer(master_a)

        # create a worker on master_b
        worker = WorkerP2P("W-B1", control_address="127.0.0.1:2103")
        master_b.register_worker(worker)

        # Master A requests 1 worker from Master B
        resp = master_a.request_help("Master_B", workers_needed=1)
        self.assertEqual(resp.get("type"), "response_accepted")

        # allow redirects to run
        time.sleep(0.5)

        # after redirect, worker should be registered on master_a
        self.assertIn("W-B1", master_a.worker_registry)
        self.assertIn("W-B1", master_b.borrowed_workers or {})


if __name__ == "__main__":
    unittest.main()
