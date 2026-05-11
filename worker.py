import json
import random
import shutil
import socket
import threading
import time
import uuid

import master

MEU_IP = "10.62.134.143"
PORT = 2003
WORKER_UUID = "W-123"
CONTROL_PORT = 2103

WORKERS = []

# known_workers is populated dynamically from master responses (addresses like "ip:port")
known_workers = []

MASTER = ("10.62.134.143", 2003)
MASTER_UUID = "Master_A"

falhas = 0
eleicao_em_andamento = False

# Quando estiver emprestado, guarda o master de origem para preencher SERVER_UUID.
ORIGINAL_MASTER_ADDRESS = None

local_master_thread = None
local_master_stop_event = None
state_lock = threading.Lock()


def log(msg):
    print(f"[WORKER {WORKER_UUID}] {msg}")


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre

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
        # Atualiza known_workers se o master retornar a lista de workers
        try:
            wk = resp.get("workers")
            if wk:
                with state_lock:
                    # parse "ip:port" strings
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
    global MASTER
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

        # Compatibilidade futura: ignora payloads desconhecidos sem derrubar o loop.
        log(f"Mensagem de tarefa desconhecida ignorada: {resposta}")

    except Exception as e:
        log(f"Erro no ciclo de tarefa: {e}")


def tratar_comando_controle(mensagem):
    global MASTER, ORIGINAL_MASTER_ADDRESS

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

    if msg_type == "command_release":
        original_master = payload.get("original_master_address")
        if not original_master or ":" not in original_master:
            return {
                "type": "error",
                "request_id": request_id,
                "payload": {"reason": "invalid_original_master_address"},
            }

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

    if msg_type == "announce_master":
        master_addr = payload.get("master_address")
        if master_addr and ":" in master_addr:
            host, port_str = master_addr.rsplit(":", 1)
            with state_lock:
                MASTER = (host, int(port_str))
            log(f"Announce recebido: novo master={MASTER}")
            # se havia um master local, encerre-o
            global local_master_thread, local_master_stop_event
            if local_master_thread and local_master_thread.is_alive():
                try:
                    local_master_stop_event.set()
                except Exception:
                    pass
                local_master_thread = None
                local_master_stop_event = None

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
        conn, _ = server.accept()
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
            # handler: support legacy DISK query and type-based control messages
            if mensagem.get("TASK") == "DISK":
                resposta = {"FREE": get_espaco_livre()}
            else:
                resposta = tratar_comando_controle(mensagem)
            conn.sendall((json.dumps(resposta) + "\n").encode())

        except Exception as e:
            log(f"Erro no listener de controle: {e}")
        finally:
            conn.close()


def eleicao():
    global MASTER

    log("Iniciando eleicao")
    candidatos = []

    # Consultar workers conhecidos para obter espaço em disco
    with state_lock:
        peers = list(known_workers)

    for worker in peers:
        try:
            payload = {"TASK": "DISK"}
            resposta = send_and_receive_json(worker, payload, timeout=2)
            if resposta and "FREE" in resposta:
                candidatos.append((worker[0], resposta["FREE"]))
        except Exception:
            continue

    candidatos.append((MEU_IP, get_espaco_livre()))

    novo_master_ip = max(candidatos, key=lambda x: (x[1], x[0]))[0]
    MASTER = (novo_master_ip, PORT)
    log(f"Novo master eleito: {MASTER}")

    # Se este no foi eleito master, iniciar servidor local; caso contrario, se
    # havia um master local rodando, encerra-lo para virar worker.
    global local_master_thread, local_master_stop_event
    if novo_master_ip == MEU_IP:
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

    if novo_master_ip == MEU_IP:
        anunciar()
        iniciar_master_local()


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
