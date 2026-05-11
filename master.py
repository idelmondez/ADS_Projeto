import json
import queue
import shutil
import socket
import threading
import time
import uuid
from datetime import datetime

SERVER_UUID = "Master_A"
HOST = "10.62.134.143"
PORT = 2003

# Sprint 03: limiares de saturacao/histerese.
CAPACITY = 10
RELEASE_THRESHOLD = 6

# Vizinhos para negociacao P2P (master_id, ip, porta).
NEIGHBORS = [
    ("Master_B", "192.168.1.48", 2003),
]

workers_ativos = set()
MASTER_ATUAL = None

task_queue = queue.Queue()
worker_registry = {}
registry_lock = threading.Lock()


def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre


def recv_json_line(conn):
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


def send_json_line(conn, payload):
    conn.sendall((json.dumps(payload) + "\n").encode())


def require_fields(payload, fields):
    missing = [field for field in fields if field not in payload]
    if missing:
        return False, f"missing_fields={missing}"
    return True, ""


def get_next_task_for_worker(worker_id):
    try:
        task = task_queue.get_nowait()
        with registry_lock:
            if worker_id in worker_registry:
                worker_registry[worker_id]["busy"] = True
        return {"TASK": "QUERY", "USER": task}
    except queue.Empty:
        return {"TASK": "NO_TASK"}


def handle_worker_alive_message(mensagem, addr):
    ok, err = require_fields(mensagem, ["WORKER", "WORKER_UUID"])
    if not ok:
        return {"ERROR": err}

    if mensagem.get("WORKER") != "ALIVE":
        return {"ERROR": "WORKER must be ALIVE"}

    worker_id = mensagem["WORKER_UUID"]
    original_master = mensagem.get("SERVER_UUID")
    control_address = mensagem.get("CONTROL_ADDRESS")

    with registry_lock:
        worker_registry[worker_id] = {
            "worker_id": worker_id,
            "borrowed": original_master is not None and original_master != SERVER_UUID,
            "original_master": original_master,
            "busy": False,
            "last_seen": time.time(),
            "addr": addr[0],
            "control_address": control_address,
        }

    return get_next_task_for_worker(worker_id)


def handle_worker_status_message(mensagem):
    ok, err = require_fields(mensagem, ["STATUS", "TASK", "WORKER_UUID"])
    if not ok:
        return {"ERROR": err}

    status = mensagem.get("STATUS")
    task_name = mensagem.get("TASK")
    worker_id = mensagem.get("WORKER_UUID")

    if status not in {"OK", "NOK"}:
        return {"ERROR": "STATUS must be OK or NOK"}
    if task_name != "QUERY":
        return {"ERROR": "TASK must be QUERY in status report"}

    with registry_lock:
        if worker_id in worker_registry:
            worker_registry[worker_id]["busy"] = False
            worker_registry[worker_id]["last_seen"] = time.time()
            borrowed = worker_registry[worker_id]["borrowed"]
        else:
            borrowed = False

    origem = "emprestado" if borrowed else "local"
    log(f"Status recebido: worker={worker_id} origem={origem} status={status}")
    return {"STATUS": "ACK", "WORKER_UUID": worker_id}


def handle_type_message(mensagem):
    msg_type = mensagem.get("type")
    request_id = mensagem.get("request_id")
    payload = mensagem.get("payload", {})

    if msg_type == "request_help":
        ok, err = require_fields(payload, ["master_id", "current_load", "capacity", "workers_needed"])
        if not ok:
            return {
                "type": "response_rejected",
                "request_id": request_id,
                "payload": {"reason": err},
            }

        with registry_lock:
            idle_workers = [
                worker
                for worker in worker_registry.values()
                if not worker["borrowed"] and not worker["busy"]
            ]

        workers_needed = int(payload["workers_needed"])
        high_load = task_queue.qsize() > int(CAPACITY * 0.8)

        if high_load:
            return {
                "type": "response_rejected",
                "request_id": request_id,
                "payload": {"reason": "high_load"},
            }

        if not idle_workers:
            return {
                "type": "response_rejected",
                "request_id": request_id,
                "payload": {"reason": "no_workers_available"},
            }

        offered = idle_workers[: max(0, workers_needed)]
        details = []
        for worker in offered:
            details.append(
                {
                    "id": worker["worker_id"],
                    "address": worker.get("control_address") or "",
                }
            )

        return {
            "type": "response_accepted",
            "request_id": request_id,
            "payload": {
                "workers_offered": len(details),
                "worker_details": details,
            },
        }

    if msg_type == "register_temporary_worker":
        ok, err = require_fields(payload, ["worker_id", "original_master_address"])
        if not ok:
            return {
                "type": "error",
                "request_id": request_id,
                "payload": {"reason": err},
            }

        worker_id = payload["worker_id"]
        with registry_lock:
            worker_registry[worker_id] = {
                "worker_id": worker_id,
                "borrowed": True,
                "original_master": payload["original_master_address"],
                "busy": False,
                "last_seen": time.time(),
                "addr": None,
                "control_address": None,
            }

        log(f"Worker temporario registrado: {worker_id} de {payload['original_master_address']}")
        return {
            "type": "register_ack",
            "request_id": request_id,
            "payload": {"status": "ACK"},
        }

    if msg_type == "notify_worker_returned":
        worker_id = payload.get("worker_id")
        if worker_id:
            with registry_lock:
                worker_registry.pop(worker_id, None)
            log(f"Worker devolvido removido da lista local: {worker_id}")
        return {
            "type": "notify_ack",
            "request_id": request_id,
            "payload": {"status": "ACK"},
        }

    log(f"Mensagem type desconhecida ignorada: {msg_type}")
    return {
        "type": "error",
        "request_id": request_id,
        "payload": {"reason": "unknown_type"},
    }


def handle_legacy_message(mensagem):
    global MASTER_ATUAL

    task = mensagem.get("TASK")

    if task == "HEARTBEAT":
        return {
            "SERVER_UUID": SERVER_UUID,
            "TASK": "HEARTBEAT",
            "RESPONSE": "ALIVE",
        }

    if task == "REGISTER":
        worker_ip = mensagem.get("WORKER")
        if worker_ip:
            workers_ativos.add(worker_ip)
            log(f"Workers ativos (legacy): {workers_ativos}")
        return {"STATUS": "REGISTERED"}

    if task == "DISK":
        return {"FREE": get_espaco_livre()}

    if task == "NEW_MASTER":
        MASTER_ATUAL = mensagem.get("MASTER")
        log(f"Novo master definido (legacy): {MASTER_ATUAL}")
        return {"STATUS": "ACK"}

    if mensagem.get("WORKER") == "ALIVE":
        return handle_worker_alive_message(mensagem, ("", 0))

    if task == "QUERY" and "STATUS" in mensagem:
        return handle_worker_status_message(mensagem)

    if "STATUS" in mensagem and "WORKER_UUID" in mensagem:
        return handle_worker_status_message(mensagem)

    return {"RESPONSE": "INVALID"}


def handle_client(conn, addr):
    try:
        mensagem = recv_json_line(conn)
        if not mensagem:
            return

        log(f"Conexao de {addr} mensagem={mensagem}")

        if "type" in mensagem:
            resposta = handle_type_message(mensagem)
        elif mensagem.get("WORKER") == "ALIVE":
            resposta = handle_worker_alive_message(mensagem, addr)
        elif "STATUS" in mensagem and "WORKER_UUID" in mensagem:
            resposta = handle_worker_status_message(mensagem)
        else:
            resposta = handle_legacy_message(mensagem)

        send_json_line(conn, resposta)

    except json.JSONDecodeError:
        log("Erro: JSON invalido recebido")
    except Exception as e:
        log(f"Erro: {e}")
    finally:
        conn.close()


def request_help_from_neighbor(neighbor):
    neighbor_id, ip, port = neighbor
    request_id = str(uuid.uuid4())
    payload = {
        "type": "request_help",
        "request_id": request_id,
        "payload": {
            "master_id": SERVER_UUID,
            "current_load": task_queue.qsize(),
            "capacity": CAPACITY,
            "workers_needed": max(1, task_queue.qsize() - CAPACITY),
        },
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((ip, port))
            send_json_line(s, payload)
            response = recv_json_line(s)
            log(
                f"Negociacao com {neighbor_id}: request_id={request_id} "
                f"response={response}"
            )
            return response
    except Exception as e:
        log(f"Falha ao negociar com {neighbor_id} ({ip}:{port}): {e}")
        return None


def monitor_saturation_loop():
    while True:
        current_load = task_queue.qsize()
        if current_load > CAPACITY:
            log(f"Saturacao detectada: load={current_load} capacity={CAPACITY}")
            for neighbor in NEIGHBORS:
                response = request_help_from_neighbor(neighbor)
                if response and response.get("type") == "response_accepted":
                    break
        time.sleep(2)


def seed_task_loop():
    users = ["Ana", "Bruno", "Carlos", "Daniela", "Eva", "Fabio", "Giulia", "Henrique"]
    idx = 0
    while True:
        task_queue.put(users[idx % len(users)])
        idx += 1
        time.sleep(1.5)


def iniciar_master(host, port=2003):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(10)

    log(f"MASTER ATIVO EM {host}:{port} ({SERVER_UUID})")

    threading.Thread(target=monitor_saturation_loop, daemon=True).start()
    threading.Thread(target=seed_task_loop, daemon=True).start()

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    iniciar_master(HOST, PORT)
