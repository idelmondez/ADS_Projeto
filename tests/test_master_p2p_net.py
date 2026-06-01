import time
import unittest
import socket

from master_p2p_net import MasterNet
from worker_p2p_net import WorkerNet


class MasterP2PNetTests(unittest.TestCase):
    def test_request_help_over_tcp_and_redirect(self):
        master_a = MasterNet("Master_A", host="127.0.0.1", port=12000)
        master_b = MasterNet("Master_B", host="127.0.0.1", port=12001)

        master_a.start()
        master_b.start()

        worker = WorkerNet("W-B1", control_host="127.0.0.1", control_port=0)
        worker.start()

        # register worker info on master_b
        with master_b.lock:
            master_b.worker_registry["W-B1"] = (worker.control_host, worker.control_port)

        # master_a requests help from master_b
        resp = master_a.send_request_help("127.0.0.1", 12001, workers_needed=1)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.get("type"), "response_accepted")

        # allow redirects to happen (worker should receive redirect)
        time.sleep(0.2)
        self.assertIsNotNone(worker.pending_new_master)

        master_a.stop()
        master_b.stop()
        worker.stop()


if __name__ == "__main__":
    unittest.main()
