import socket
import json
import time
import shutil
import master

MEU_IP = "192.168.1.47"
PORT = 2003

WORKERS = [
    ("192.168.1.47", 2003),
]

MASTER = ("192.168.1.47", 2003)

falhas = 0
eleicao_em_andamento = False


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre


def registrar():
    try:
        print("Registrando no master:", MASTER)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(MASTER)

        payload = {
            "TASK": "REGISTER",
            "WORKER": MEU_IP
        }

        s.sendall((json.dumps(payload) + "\n").encode())
        s.close()

    except Exception as e:
        print("Erro ao registrar:", e)


def heartbeat():
    global falhas, eleicao_em_andamento, MASTER

    try:
        print("Heartbeat para:", MASTER)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(MASTER)

        payload = {
            "TASK": "HEARTBEAT"
        }

        s.sendall((json.dumps(payload) + "\n").encode())

        resposta = s.recv(1024).decode()

        if resposta:
            falhas = 0
            eleicao_em_andamento = False

        s.close()

    except Exception as e:
        falhas += 1
        print("Falha:", falhas, e)

    if falhas >= 4 and not eleicao_em_andamento:
        eleicao_em_andamento = True
        eleicao()


def eleicao():
    global MASTER

    print("Iniciando eleição")

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

    print("Novo master:", MASTER)

    if novo_master_ip == MEU_IP:
        anunciar()
        iniciar_master_local()


def anunciar():
    print("Anunciando novo master")

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
    print("Este nó virou master")
    master.iniciar_master(MEU_IP, PORT)


registrar()

while True:
    heartbeat()
    time.sleep(3)