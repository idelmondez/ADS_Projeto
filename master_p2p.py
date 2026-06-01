import threading
import time
import uuid


class MasterP2P:
    def __init__(self, master_id, host="127.0.0.1", port=10000):
        self.master_id = master_id
        self.host = host
        self.port = int(port)
        self.task_queue = []
        self.worker_registry = {}  # worker_id -> worker obj
        self.borrowed_workers = {}  # worker_id -> original_master_id
        self.master_peers = {}  # master_id -> MasterP2P object (in-memory peer registry)
        self.lock = threading.Lock()

    # Peer management (in-memory simulation)
    def add_peer(self, peer_master):
        self.master_peers[peer_master.master_id] = peer_master

    def register_worker(self, worker):
        with self.lock:
            self.worker_registry[worker.worker_id] = worker

    def unregister_worker(self, worker_id):
        with self.lock:
            self.worker_registry.pop(worker_id, None)

    # Request help from a specific peer (synchronous simulation)
    def request_help(self, peer_master_id, workers_needed=1):
        req_id = str(uuid.uuid4())
        peer = self.master_peers.get(peer_master_id)
        if not peer:
            return {"status": "error", "reason": "peer_not_found"}

        payload = {
            "type": "request_help",
            "request_id": req_id,
            "payload": {
                "master_id": self.master_id,
                "current_load": len(self.task_queue),
                "capacity": 100,
                "workers_needed": workers_needed,
            },
        }

        # In a networked implementation we'd open a TCP connection; here call handler directly
        resp = peer.handle_type_message(payload, sender=self)
        return resp

    def handle_type_message(self, message, sender=None):
        msg_type = message.get("type")
        request_id = message.get("request_id")
        payload = message.get("payload", {})

        if msg_type == "request_help":
            # evaluate idle workers
            with self.lock:
                idle = [w for w in self.worker_registry.values() if not getattr(w, "busy", False)]

            workers_needed = int(payload.get("workers_needed", 0))
            if not idle:
                return {"type": "response_rejected", "request_id": request_id, "payload": {"reason": "no_workers_available"}}

            offered = idle[:workers_needed]
            details = [{"id": w.worker_id, "address": w.control_address} for w in offered]

            # Reserve offered workers (mark busy) but do not transfer yet
            with self.lock:
                for w in offered:
                    w.busy = True

            response = {"type": "response_accepted", "request_id": request_id, "payload": {"workers_offered": len(details), "worker_details": details}}

            # schedule redirects asynchronously
            threading.Thread(target=self._perform_redirects, args=(offered, sender, request_id), daemon=True).start()
            return response

        if msg_type == "notify_worker_returned":
            wid = payload.get("worker_id")
            with self.lock:
                # cleanup borrowed records
                self.borrowed_workers.pop(wid, None)
            return {"type": "notify_ack", "request_id": request_id, "payload": {"status": "ACK"}}

        return {"type": "error", "request_id": request_id, "payload": {"reason": "unknown_type"}}

    def _perform_redirects(self, offered_workers, requester_master, request_id):
        # For each offered worker, instruct it to redirect
        for w in offered_workers:
            try:
                print(f"[master {self.master_id}] performing redirect of {w.worker_id} to {getattr(requester_master,'master_id',None)}")
                # send command_redirect to worker (in-memory call)
                new_master_addr = f"{requester_master.host}:{requester_master.port}"
                cmd = {"type": "command_redirect", "request_id": request_id, "payload": {"new_master_address": new_master_addr}}
                w.handle_command_redirect(cmd)
                # record as borrowed locally before unregistering
                with self.lock:
                    self.borrowed_workers[w.worker_id] = f"{self.host}:{self.port}"
                    # remove from local registry without reacquiring the same lock
                    self.worker_registry.pop(w.worker_id, None)
                # worker should connect to requester_master and register (simulate connection)
                print(f"[master {self.master_id}] worker has perform_connect: {hasattr(w,'perform_connect_to_new_master')}")
                if hasattr(w, "perform_connect_to_new_master"):
                    try:
                        w.perform_connect_to_new_master(requester_master)
                    except Exception as e:
                        print(f"redirect perform_connect failed: {e}")
                        # fallback to direct register
                        requester_master.register_temporary_worker(w.worker_id, f"{self.host}:{self.port}", worker_obj=w)
                else:
                    requester_master.register_temporary_worker(w.worker_id, f"{self.host}:{self.port}", worker_obj=w)
            except Exception as e:
                print(f"[master {self.master_id}] redirect error for {w.worker_id}: {e}")
                # on failure, release busy flag and cleanup borrowed marker
                with self.lock:
                    w.busy = False
                    self.borrowed_workers.pop(w.worker_id, None)

    def register_temporary_worker(self, worker_id, original_master_address, worker_obj=None):
        # Called when a redirected worker connects to this master
        with self.lock:
            print(f"[master {self.master_id}] registering temporary worker {worker_id} from {original_master_address}")
            if worker_obj:
                self.worker_registry[worker_id] = worker_obj
            else:
                # placeholder entry
                self.worker_registry[worker_id] = type("W", (), {"worker_id": worker_id, "control_address": None, "busy": False})()
            self.borrowed_workers[worker_id] = original_master_address

    def release_worker_back(self, worker_id, original_master_id):
        # instruct worker to return (in-memory simulation)
        with self.lock:
            w = self.worker_registry.get(worker_id)
            if not w:
                return False
            # send command_release
            cmd = {"type": "command_release", "request_id": str(uuid.uuid4()), "payload": {"original_master_address": original_master_id}}
            w.handle_command_release(cmd)
            # notify original master
            orig = self.master_peers.get(original_master_id)
            if orig:
                orig.handle_type_message({"type": "notify_worker_returned", "request_id": str(uuid.uuid4()), "payload": {"worker_id": worker_id}}, sender=self)
            # remove from local registry
            self.unregister_worker(worker_id)
            self.borrowed_workers.pop(worker_id, None)
            return True
