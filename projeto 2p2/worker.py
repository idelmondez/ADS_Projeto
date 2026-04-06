import socket
import json
import time
import shutil

MASTER = ("192.168.56.1", 2003)

WORKERS = [
    ("172.31.88.25", 2003),
    ("172.31.88.26", 2003),
    ("172.31.88.27", 2003),
]

SERVER_UUID = "worker-01"
falhas = 0


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre


def enviar_heartbeat():
    global falhas

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(MASTER)

        payload = {
            "SERVER_UUID": "master-01",
            "TASK": "HEARTBEAT"
        }

        s.sendall((json.dumps(payload) + "\n").encode())

        resposta = s.recv(1024).decode().strip()

        if resposta:
            print("[OK] Master ativo")
            falhas = 0
        else:
            falhas += 1

        s.close()

    except:
        falhas += 1
        print(f"[ERRO] Falha {falhas}/4")

    if falhas >= 4:
        eleicao()


def eleicao():
    global MASTER

    print("\n[!] Iniciando eleição...")

    candidatos = []

    for worker in WORKERS:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect(worker)

            payload = {"TASK": "DISK"}
            s.sendall((json.dumps(payload) + "\n").encode())

            resposta = json.loads(s.recv(1024).decode())
            candidatos.append((worker, resposta["FREE"]))

            s.close()

        except:
            continue

    # inclui ele mesmo
    meu_espaco = get_espaco_livre()
    candidatos.append((("self", 2003), meu_espaco))

    # escolhe maior
    novo_master = max(candidatos, key=lambda x: x[1])[0]

    print(f"[ELEIÇÃO] Novo master: {novo_master}")

    if novo_master == ("self", 2003):
        print("[MASTER] Eu sou o novo master!")
        iniciar_master_local()
    else:
        MASTER = novo_master


def iniciar_master_local():
    import threading

    def handle_client(conn, addr):
        data = conn.recv(1024).decode().strip()

        if "HEARTBEAT" in data:
            resposta = {
                "SERVER_UUID": "master-01",
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }
            conn.sendall((json.dumps(resposta) + "\n").encode())

        elif "DISK" in data:
            resposta = {"FREE": get_espaco_livre()}
            conn.sendall((json.dumps(resposta) + "\n").encode())

        conn.close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", 2003))
    server.listen()

    print("[MASTER] Novo master iniciado!")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


# LOOP PRINCIPAL
while True:
    enviar_heartbeat()
    time.sleep(5)