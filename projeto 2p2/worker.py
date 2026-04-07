import socket
import json
import time
import shutil

MEU_IP = '10.62.206.17' 
PORT = 2003

WORKERS = [
    ("10.62.206.17", 2003),
    ("10.62.206.207", 2003),
]

MASTER = ("172.31.88.25", 2003)

falhas = 0
eleicao_em_andamento = False


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre


def enviar_heartbeat():
    global falhas, eleicao_em_andamento

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(MASTER)

        payload = {"TASK": "HEARTBEAT"}
        s.sendall((json.dumps(payload) + "\n").encode())

        resposta = s.recv(1024).decode()

        if resposta:
            print("[OK] Master ativo:", MASTER)
            falhas = 0
            eleicao_em_andamento = False

        s.close()

    except:
        falhas += 1
        print(f"[ERRO] Falha {falhas}/4")

    if falhas >= 4 and not eleicao_em_andamento:
        eleicao_em_andamento = True
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

            candidatos.append((worker[0], resposta["FREE"]))
            s.close()

        except:
            continue

    candidatos.append((MEU_IP, get_espaco_livre()))

    novo_master_ip = max(candidatos, key=lambda x: (x[1], x[0]))[0]

    MASTER = (novo_master_ip, PORT)

    print("[ELEIÇÃO] Novo master:", MASTER)

    if novo_master_ip == MEU_IP:
        anunciar_master()
        iniciar_master_local()


def anunciar_master():
    print("[MASTER] Anunciando novo master...")

    for worker in WORKERS:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(worker)

            payload = {
                "TASK": "NEW_MASTER",
                "MASTER": MEU_IP
            }

            s.sendall((json.dumps(payload) + "\n").encode())
            s.close()

        except:
            continue


def iniciar_master_local():
    import threading

    def handle_client(conn, addr):
        data = conn.recv(1024).decode().strip()

        if "HEARTBEAT" in data:
            resposta = {
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }

        elif "DISK" in data:
            resposta = {"FREE": get_espaco_livre()}

        elif "NEW_MASTER" in data:
            global MASTER
            mensagem = json.loads(data)
            MASTER = (mensagem["MASTER"], PORT)
            resposta = {"STATUS": "ACK"}

        else:
            resposta = {"RESPONSE": "INVALID"}

        conn.sendall((json.dumps(resposta) + "\n").encode())
        conn.close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MEU_IP, PORT))
    server.listen()

    print("[MASTER] Este nó agora é o MASTER!")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


while True:
    enviar_heartbeat()
    time.sleep(5)