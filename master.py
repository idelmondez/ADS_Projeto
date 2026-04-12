import socket
import json
import threading
import shutil

SERVER_UUID = "master-01"

workers_ativos = set()
MASTER_ATUAL = None


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre


def handle_client(conn, addr):
    global MASTER_ATUAL

    try:
        data = conn.recv(1024).decode().strip()
        if not data:
            return

        mensagem = json.loads(data)

        print("Conexão de:", addr)
        print("Mensagem:", mensagem)

        if mensagem.get("TASK") == "HEARTBEAT":
            resposta = {
                "SERVER_UUID": SERVER_UUID,
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }

        elif mensagem.get("TASK") == "REGISTER":
            worker_ip = mensagem.get("WORKER")
            if worker_ip:
                workers_ativos.add(worker_ip)
                print("Workers ativos:", workers_ativos)

            resposta = {
                "STATUS": "REGISTERED"
            }

        elif mensagem.get("TASK") == "DISK":
            resposta = {
                "FREE": get_espaco_livre()
            }

        elif mensagem.get("TASK") == "NEW_MASTER":
            MASTER_ATUAL = mensagem.get("MASTER")
            print("Novo master definido:", MASTER_ATUAL)

            resposta = {
                "STATUS": "ACK"
            }

        else:
            resposta = {
                "RESPONSE": "INVALID"
            }

        conn.sendall((json.dumps(resposta) + "\n").encode())

    except Exception as e:
        print("Erro:", e)

    finally:
        conn.close()


def iniciar_master(host, port=2003):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)

    print(f"MASTER ATIVO EM {host}:{port}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


if __name__ == "__main__":
    iniciar_master("192.168.1.47")