import socket
import json
import threading
import shutil

HOST = '192.168.56.1'
PORT = 2003
SERVER_UUID = "master-01"

def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre

def handle_client(conn, addr):
    print(f"\n[+] Conexão de {addr}")

    try:
        data = conn.recv(1024).decode().strip()

        if not data:
            return

        print("[>] Recebido:", data)

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

        else:
            resposta = {
                "SERVER_UUID": SERVER_UUID,
                "RESPONSE": "INVALID"
            }

        conn.sendall((json.dumps(resposta) + "\n").encode())

        print("[<] Enviado:", resposta)

    except Exception as e:
        print(f"[ERRO] {e}")

    finally:
        conn.close()
        print(f"[-] Conexão com {addr} encerrada")


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