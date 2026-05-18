import json
import os
import random
import shutil
import socket
import threading
import time

import master

MEU_IP = None
PORT = 2003
WORKER_UUID = "W-123"
CONTROL_PORT = 2103
MASTER = None
MASTER_UUID = "Master_A"
WORKERS = []
known_workers = []

falhas = 0
eleicao_em_andamento = False
ORIGINAL_MASTER_ADDRESS = None
local_master_thread = None
local_master_stop_event = None
state_lock = threading.Lock()


def detect_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


env_meu = os.getenv("MEU_IP")
if env_meu:
    MEU_IP = env_meu

env_worker = os.getenv("WORKER_UUID")
if env_worker:
    WORKER_UUID = env_worker

env_control = os.getenv("CONTROL_PORT")
if env_control:
    try:
        CONTROL_PORT = int(env_control)
    except Exception:
        pass

env_master_host = os.getenv("MASTER_HOST")
env_master_port = os.getenv("MASTER_PORT")
if env_master_host and env_master_port:
    try:
        MASTER = (env_master_host, int(env_master_port))
    except Exception:
        pass

if not MEU_IP:
    MEU_IP = detect_local_ip()

if MASTER is None:
    MASTER = (MEU_IP, PORT)


def log(msg):
    print(f"[WORKER {WORKER_UUID}] {msg}")


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre


def get_espaco_livre_mb():
    return get_espaco_livre() // (1024 * 1024)


def send_and_receive_json(server, payload, timeout=5):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect(server)
        s.sendall((json.dumps(payload) + "\n").encode())

        data = b""
        while b"\n" not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk

        if not data:
            return None

        line = data.split(b"\n", 1)[0].decode().strip()
        if not line:
            return None

        resp = json.loads(line)
        try:
            wk = resp.get("workers")
            if wk:
                with state_lock:
                    known_workers.clear()
                    for a in wk:
                        if isinstance(a, str) and ":" in a:
                            host, port = a.rsplit(":", 1)
                            try:
                                known_workers.append((host, int(port)))
                            except Exception:
                                continue
        except Exception:
            pass

        return resp


def registrar_legacy():
    try:
        log(f"Registrando no master (legacy): {MASTER}")
        payload = {"TASK": "REGISTER", "WORKER": MEU_IP}
        send_and_receive_json(MASTER, payload, timeout=3)
    except Exception as e:
        log(f"Erro ao registrar (legacy): {e}")


def heartbeat():
    global falhas, eleicao_em_andamento

    with state_lock:
        current_master = MASTER

    try:
        payload = {"SERVER_UUID": MASTER_UUID, "TASK": "HEARTBEAT"}
        resposta = send_and_receive_json(current_master, payload, timeout=5)

        if (
            resposta
            and resposta.get("SERVER_UUID")
            and resposta.get("TASK") == "HEARTBEAT"
            and resposta.get("RESPONSE") == "ALIVE"
        ):
            falhas = 0
            eleicao_em_andamento = False
            return True

        falhas += 1
        log(f"Heartbeat invalido. falhas={falhas} resposta={resposta}")
        return False

    except Exception as e:
        falhas += 1
        log(f"Falha heartbeat={falhas} erro={e}")
        return False


def solicitar_tarefa_e_processar():
    global ORIGINAL_MASTER_ADDRESS

    with state_lock:
        current_master = MASTER
        borrowed_from = ORIGINAL_MASTER_ADDRESS

    payload = {
        "WORKER": "ALIVE",
        "WORKER_UUID": WORKER_UUID,
        "CONTROL_ADDRESS": f"{MEU_IP}:{CONTROL_PORT}",
    }
    if borrowed_from:
        payload["SERVER_UUID"] = borrowed_from

    try:
        resposta = send_and_receive_json(current_master, payload, timeout=5)
        if not resposta:
            return

        task = resposta.get("TASK")
        if task == "NO_TASK":
            return

        if task == "QUERY":
            user = resposta.get("USER", "unknown")
            process_time = random.uniform(0.5, 2.0)
            time.sleep(process_time)

            status = "OK" if random.random() > 0.1 else "NOK"
            report = {
                "STATUS": status,
                "TASK": "QUERY",
                "WORKER_UUID": WORKER_UUID,
            }
            ack = send_and_receive_json(current_master, report, timeout=5)
            log(f"Task para USER={user} concluida com {status}. ACK={ack}")
            return

        log(f"Mensagem de tarefa desconhecida ignorada: {resposta}")

    except Exception as e:
        log(f"Erro no ciclo de tarefa: {e}")


def tratar_comando_controle(mensagem):
    global MASTER, ORIGINAL_MASTER_ADDRESS, local_master_thread, local_master_stop_event

    msg_type = mensagem.get("type")
    request_id = mensagem.get("request_id")
    payload = mensagem.get("payload", {})

    if msg_type == "command_redirect":
        new_master_address = payload.get("new_master_address")
        if not new_master_address or ":" not in new_master_address:
            return {
                "type": "error",
                "request_id": request_id,
                "payload": {"reason": "invalid_new_master_address"},
            }

        try:
            host, port_str = new_master_address.rsplit(":", 1)
            with state_lock:
                ORIGINAL_MASTER_ADDRESS = f"{MASTER[0]}:{MASTER[1]}"
                MASTER = (host, int(port_str))
            log(f"Redirecionado para novo master={MASTER} origem={ORIGINAL_MASTER_ADDRESS}")
            return {
                "type": "redirect_ack",
                "request_id": request_id,
                "payload": {"status": "ACK"},
            }
        except Exception as e:
            log(f"Erro ao processar command_redirect: {e}")
            return {
                "type": "error",
                "request_id": request_id,
                "payload": {"reason": f"error_processing_redirect: {str(e)}"},
            }

    if msg_type == "command_release":
        original_master = payload.get("original_master_address")
        if not original_master or ":" not in original_master:
            return {
                "type": "error",
                "request_id": request_id,
                "payload": {"reason": "invalid_original_master_address"},
            }

        try:
            host, port_str = original_master.rsplit(":", 1)
            with state_lock:
                MASTER = (host, int(port_str))
                ORIGINAL_MASTER_ADDRESS = None
            log(f"Liberado para retornar ao master original={MASTER}")
            return {
                "type": "release_ack",
                "request_id": request_id,
                "payload": {"status": "ACK"},
            }
        except Exception as e:
            log(f"Erro ao processar command_release: {e}")
            return {
                "type": "error",
                "request_id": request_id,
                "payload": {"reason": f"error_processing_release: {str(e)}"},
            }

    if msg_type == "announce_master":
        master_addr = payload.get("master_address")
        if master_addr and ":" in master_addr:
            try:
                host, port_str = master_addr.rsplit(":", 1)
                with state_lock:
                    MASTER = (host, int(port_str))
                log(f"Announce recebido: novo master={MASTER}")
                if local_master_thread and local_master_thread.is_alive():
                    try:
                        local_master_stop_event.set()
                    except Exception:
                        pass
                    local_master_thread = None
                    local_master_stop_event = None
            except Exception as e:
                log(f"Erro ao processar announce_master: {e}")

        return {
            "type": "announce_ack",
            "request_id": request_id,
            "payload": {"status": "ACK"},
        }

    return {
        "type": "error",
        "request_id": request_id,
        "payload": {"reason": "unknown_type"},
    }


def controle_listener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((MEU_IP, CONTROL_PORT))
    server.listen(5)
    log(f"Canal de controle ouvindo em {MEU_IP}:{CONTROL_PORT}")

    while True:
        conn, addr = server.accept()
        try:
            data = b""
            while b"\n" not in data:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk

            if not data:
                continue

            line = data.split(b"\n", 1)[0].decode().strip()
            if not line:
                continue

            mensagem = json.loads(line)
            if mensagem.get("TASK") == "DISK":
                resposta = {
                    "FREE": get_espaco_livre(),
                    "WORKER_UUID": WORKER_UUID,
                    "CONTROL_PORT": CONTROL_PORT,
                }
            else:
                resposta = tratar_comando_controle(mensagem)

            conn.sendall((json.dumps(resposta) + "\n").encode())

        except json.JSONDecodeError as e:
            log(f"Erro ao fazer parse JSON no listener de controle: {e}")
            try:
                erro_resp = {"type": "error", "payload": {"reason": "json_decode_error"}}
                conn.sendall((json.dumps(erro_resp) + "\n").encode())
            except Exception:
                pass
        except Exception as e:
            log(f"Erro no listener de controle: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass


def eleicao():
    global MASTER, local_master_thread, local_master_stop_event

    log("Iniciando eleicao")
    candidatos = []

    with state_lock:
        peers = list(known_workers)

    for worker in peers:
        try:
            payload = {"TASK": "DISK"}
            resposta = send_and_receive_json(worker, payload, timeout=2)
            if resposta and "FREE" in resposta:
                candidatos.append((worker[0], worker[1], resposta["FREE"] // (1024 * 1024)))
        except Exception:
            continue

    candidatos.append((MEU_IP, CONTROL_PORT, get_espaco_livre_mb()))

    novo_master_ip, novo_master_port, _ = max(candidatos, key=lambda x: x[2])
    MASTER = (novo_master_ip, PORT)
    log(f"Novo master eleito: {MASTER}")

    if novo_master_ip == MEU_IP:
        anunciar()
        iniciar_master_local()
    else:
        if local_master_thread and local_master_thread.is_alive():
            log("Nodo deixou de ser master local -> encerrando servidor local")
            try:
                local_master_stop_event.set()
            except Exception:
                pass
            local_master_thread = None
            local_master_stop_event = None


def anunciar():
    log("Anunciando novo master")
    for worker in WORKERS:
        try:
            payload = {"TASK": "NEW_MASTER", "MASTER": MEU_IP}
            send_and_receive_json(worker, payload, timeout=2)
        except Exception:
            continue


def iniciar_master_local():
    global local_master_thread, local_master_stop_event
    log("Este no virou master")
    if local_master_thread and local_master_thread.is_alive():
        log("Master local ja em execucao")
        return

    stop_event = threading.Event()
    t = threading.Thread(target=master.iniciar_master, args=(MEU_IP, PORT, stop_event), daemon=True)
    t.start()
    local_master_thread = t
    local_master_stop_event = stop_event
    log("Master local iniciado em thread")


def main_loop():
    global eleicao_em_andamento

    registrar_legacy()

    while True:
        ok = heartbeat()

        if not ok and falhas >= 4 and not eleicao_em_andamento:
            eleicao_em_andamento = True
            eleicao()

        if ok:
            solicitar_tarefa_e_processar()

        time.sleep(3)


if __name__ == "__main__":
    threading.Thread(target=controle_listener, daemon=True).start()
    main_loop()
