import socket
import json
import threading
import shutil


def get_espaco_livre():
    total, usado, livre = shutil.disk_usage("/")
    return livre


def handle_client(conn, addr):
    try:
        data = conn.recv(1024).decode().strip()

        if not data:
            return

        mensagem = json.loads(data)

        print(f"\n[CONEXÃO] {addr}")

        if mensagem.get("TASK") == "HEARTBEAT":
            print("[HEARTBEAT] de", addr)
            resposta = {
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }

        elif mensagem.get("TASK") == "DISK":
            print("[DISK] consulta de", addr)
            resposta = {
                "FREE": get_espaco_livre()
            }

        elif mensagem.get("TASK") == "NEW_MASTER":
            print("[INFO] Novo master anunciado:", mensagem.get("MASTER"))
            resposta = {"STATUS": "ACK"}

        else:
            resposta = {"RESPONSE": "INVALID"}

        conn.sendall((json.dumps(resposta) + "\n").encode())

    except Exception as e:
        print("[ERRO]", e)

    finally:
        conn.close()
        print("[DESCONECTADO]", addr)


def iniciar_master(host, port=2003):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((host, port))
    server.listen(5)

    print(f"\n🔥 MASTER rodando em {host}:{port}\n")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


if __name__ == "__main__":
    iniciar_master("10.62.206.17")