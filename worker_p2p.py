class WorkerP2P:
    def __init__(self, worker_id, control_address=None):
        self.worker_id = worker_id
        self.control_address = control_address or "127.0.0.1:0"
        self.master = None
        self.busy = False

    def handle_command_redirect(self, message):
        # message.payload.new_master_address
        payload = message.get("payload", {})
        new_addr = payload.get("new_master_address")
        # In a real implementation the worker would close and reconnect; here we just store target
        self._pending_new_master = new_addr

    def perform_connect_to_new_master(self, master_obj):
        # simulate connecting to master and sending register_temporary_worker
        self.master = master_obj
        print(f"[worker {self.worker_id}] connecting to master {getattr(master_obj,'master_id',None)}")
        # notify master by calling its register_temporary_worker
        master_obj.register_temporary_worker(self.worker_id, original_master_address="unknown", worker_obj=self)

    def handle_command_release(self, message):
        payload = message.get("payload", {})
        orig = payload.get("original_master_address")
        # simulate immediate reconnection to original (no-op here)
        self.master = None
