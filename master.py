import json
import queue
import os
import shutil
import socket
import threading
import time
import uuid
from datetime import datetime

SERVER_UUID = "Master_A"
HOST = None
PORT = 2003
DISCOVERY_PORT = 2103

env_host = os.getenv("MASTER_HOST")
env_port = os.getenv("MASTER_PORT")
if env_host:
    HOST = env_host
if env_port:
    try:
        PORT = int(env_port)
    except Exception:
        pass

# Auto-detect local IP when MASTER_HOST is not provided
def detect_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


if not HOST:
    HOST = detect_local_ip()

# Sprint 03: limiares de saturacao/histerese.
CAPACITY = 10
RELEASE_THRESHOLD = 6

# Porta de comunicacao entre masters.
MASTER_COMM_PORT = 10000

# Vizinhos para negociacao P2P (master_id, ip, porta).
NEIGHBORS = [
    ("Master_B", "192.168.1.48", MASTER_COMM_PORT),
]

workers_ativos = set()
MASTER_ATUAL = None

task_queue = queue.Queue()
worker_registry = {}
registry_lock = threading.Lock()
borrowed_workers = {}  # Rastreia workers emprestados: worker_id -> original_master_address
borrowed_workers_lock = threading.Lock()
known_master_peers = set()
master_peers_lock = threading.Lock()

for _, peer_host, peer_port in NEIGHBORS:
    known_master_peers.add((peer_host, peer_port))


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


def register_master_peer(peer_host, peer_port=MASTER_COMM_PORT):
    if not peer_host:
        return False

    try:
        peer_port = int(peer_port)
    except Exception:
        peer_port = MASTER_COMM_PORT

    with master_peers_lock:
        before = len(known_master_peers)
        known_master_peers.add((peer_host, peer_port))
        return len(known_master_peers) > before


def register_master_peer_from_address(master_address):
    if not master_address:
        return False

    if isinstance(master_address, str) and ":" in master_address:
        host, port_str = master_address.rsplit(":", 1)
        try:
            port = int(port_str)
        except Exception:
            port = MASTER_COMM_PORT
        return register_master_peer(host, port)

    return register_master_peer(master_address, MASTER_COMM_PORT)


def list_master_peers():
    with master_peers_lock:
        return sorted(known_master_peers)


def udp_discovery_listener(stop_event, host="0.0.0.0", port=DISCOVERY_PORT):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1.0)
    except Exception as e:
        log(f"Erro ao iniciar listener UDP de discovery: {e}")
        return

    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            continue
        except Exception:
            continue

        try:
            payload = json.loads(data.decode().strip())
        except Exception:
            continue

        if payload.get("TASK") != "HEARTBEAT":
            continue

        response = {
            "SERVER_UUID": SERVER_UUID,
            "TASK": "HEARTBEAT",
            "RESPONSE": "ALIVE",
        }
        try:
            sock.sendto((json.dumps(response) + "\n").encode(), addr)
        except Exception:
            continue

    try:
        sock.close()
    except Exception:
        pass


def handle_master_comm_client(conn, addr, peer_port=MASTER_COMM_PORT):
    try:
        mensagem = recv_json_line(conn)
        if not mensagem:
            return

        register_master_peer(addr[0], peer_port)
        payload = mensagem.get("payload", {})
        if isinstance(payload, dict):
            register_master_peer_from_address(payload.get("master_address"))

        log(f"Conexao de master-peer {addr} mensagem={mensagem}")

        if "type" not in mensagem:
            send_json_line(
                conn,
                {
                    "type": "error",
                    "request_id": mensagem.get("request_id"),
                    "payload": {"reason": "missing_type"},
                },
            )
            return

        resp = handle_type_message(mensagem)
        send_json_line(conn, resp)

    except Exception as e:
        log(f"Erro no handle_master_comm_client: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def iniciar_master_comm_listener(stop_event, host="0.0.0.0", port=MASTER_COMM_PORT):
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(5)
    except Exception as e:
        log(f"Erro ao iniciar listener de comunicacao entre masters: {e}")
        return

    log(f"Comunicação entre masters ouvindo em {host}:{port}")

    while not stop_event.is_set():
        try:
            conn, addr = server.accept()
            threading.Thread(
                target=handle_master_comm_client,
                args=(conn, addr, port),
                daemon=True,
            ).start()
        except Exception as e:
            log(f"Erro no master comm accept: {e}")
            time.sleep(0.1)

    try:
        server.close()
    except Exception:
        pass


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


def list_worker_addresses():
    with registry_lock:
        out = []
        for w in worker_registry.values():
            addr = w.get("control_address")
            if addr:
                out.append(addr)
            else:
                a = w.get("addr")
                if a:
                    out.append(f"{a}:{PORT}")
        return out


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

    if msg_type == "announce_master":
        # Handler para anúncio de novo master (comunicação entre masters)
        master_addr = payload.get("master_address")
        if master_addr:
            register_master_peer_from_address(master_addr)
            log(f"Anúncio de novo master recebido: {master_addr}")
        return {
            "type": "announce_ack",
            "request_id": request_id,
            "payload": {"status": "ACK"},
        }

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

        # Armazenar informações sobre os workers sendo ofertados para posterior envio de command_redirect
        response_msg = {
            "type": "response_accepted",
            "request_id": request_id,
            "payload": {
                "workers_offered": len(details),
                "worker_details": details,
            },
        }

        # Agendar envio de command_redirect em thread separada
        def send_redirects():
            requester_id = payload.get("master_id")
            requester_load = payload.get("current_load")
            requester_capacity = payload.get("capacity")

            # Calcular novo endereço do master solicitante (IP:PORT)
            # Isso será enviado pelo handler que recebeu a requisição
            log(f"Agendando redirecionamento de {len(offered)} workers para Master {requester_id}")
            # O sender da requisição será responsável por informar seu endereço
            # Por enquanto, apenas registramos a intenção

        threading.Thread(target=send_redirects, daemon=True).start()

        return response_msg

    if msg_type == "register_temporary_worker":
        ok, err = require_fields(payload, ["worker_id", "original_master_address"])
        if not ok:
            return {
                "type": "error",
                "request_id": request_id,
                "payload": {"reason": err},
            }

        worker_id = payload["worker_id"]
        original_master = payload["original_master_address"]
        with registry_lock:
            worker_registry[worker_id] = {
                "worker_id": worker_id,
                "borrowed": True,
                "original_master": original_master,
                "busy": False,
                "last_seen": time.time(),
                "addr": None,
                "control_address": None,
            }

        with borrowed_workers_lock:
            borrowed_workers[worker_id] = original_master

        log(f"Worker temporario registrado: {worker_id} de {original_master}. Total emprestados: {len(borrowed_workers)}")
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
            with borrowed_workers_lock:
                borrowed_workers.pop(worker_id, None)
            log(f"Worker devolvido removido da lista local: {worker_id}. Total emprestados: {len(borrowed_workers)}")
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
            "workers": list_worker_addresses(),
        }

    if task == "REGISTER":
        worker_ip = mensagem.get("WORKER")
        if worker_ip:
            workers_ativos.add(worker_ip)
            log(f"Workers ativos (legacy): {workers_ativos}")
        return {"STATUS": "REGISTERED", "workers": list_worker_addresses()}

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
            resp = handle_type_message(mensagem)
            send_json_line(conn, resp)
            return

        # legacy handling
        resp = handle_legacy_message(mensagem)
        send_json_line(conn, resp)

    except Exception as e:
        log(f"Erro no handle_client: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def monitor_borrowed_workers_loop():
    while True:
        with borrowed_workers_lock:
            for wid, orig in list(borrowed_workers.items()):
                # Optionally implement health checks
                pass
        time.sleep(5)


def iniciar_master(host, port, stop_event):
    log(f"Master iniciando em {host}:{port}")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)

    threading.Thread(target=monitor_borrowed_workers_loop, daemon=True).start()
    threading.Thread(target=iniciar_master_comm_listener, args=(stop_event,), daemon=True).start()
    threading.Thread(target=udp_discovery_listener, args=(stop_event,), daemon=True).start()

    while not stop_event.is_set():
        try:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
        except Exception as e:
            log(f"Erro no master accept: {e}")
            time.sleep(0.1)

    try:
        server.close()
    except Exception:
        pass


if __name__ == "__main__":
    stop_event = threading.Event()
    iniciar_master(HOST, PORT, stop_event)
