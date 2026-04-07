import socket
import json
import threading
import shutil

HOST = '10.62.206.17'
PORT = 2003
SERVER_UUID = HOST


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre


def handle_client(conn, addr):
    try:
        data = conn.recv(1024).decode().strip()
        if not data:
            return

        mensagem = json.loads(data)

        if mensagem.get("TASK") == "HEARTBEAT":
            resposta = {
                "SERVER_UUID": SERVER_UUID,
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }

        elif mensagem.get("TASK") == "DISK":
            resposta = {
                "SERVER_UUID": SERVER_UUID,
                "TASK": "DISK",
                "FREE": get_espaco_livre()
            }

        elif mensagem.get("TASK") == "NEW_MASTER":
            resposta = {"STATUS": "ACK"}

        else:
            resposta = {"RESPONSE": "INVALID"}

        conn.sendall((json.dumps(resposta) + "\n").encode())

    except Exception as e:
        print("Erro:", e)

    finally:
        conn.close()


def iniciar_master(host):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((host, PORT))
    server.listen(5)

    print(f"[MASTER] Rodando em {host}:{PORT}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


if __name__ == "__main__":
    iniciar_master(HOST)