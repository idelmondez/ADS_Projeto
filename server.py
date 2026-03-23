import socket
import json
import threading

HOST = '172.31.88.25'
PORT = 2003
SERVER_UUID = "master-01"

def handle_client(conn, addr):
    print(f"\nConexão recebida de {addr}")

    try:
        data = conn.recv(1024).decode().strip()

        if not data:
            print("Nenhum dado recebido.")
            return

        print("Mensagem recebida:")
        print(data)

        mensagem_json = json.loads(data)

        if (
            mensagem_json.get("TASK") == "HEARTBEAT"
            and mensagem_json.get("SERVER_UUID") == SERVER_UUID
        ):
            resposta = {
                "SERVER_UUID": SERVER_UUID,
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }
        else:
            resposta = {
                "SERVER_UUID": SERVER_UUID,
                "TASK": "HEARTBEAT",
                "RESPONSE": "INVALID"
            }

        conn.sendall((json.dumps(resposta) + "\n").encode())

        print("Resposta enviada:")
        print(resposta)

    except json.JSONDecodeError:
        print("Erro: JSON inválido recebido.")

    except Exception as e:
        print(f"Erro: {e}")

    finally:
        conn.close()
        print(f"Conexão com {addr} encerrada.")


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Evita erro ao reiniciar
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server.bind((HOST, PORT))
server.listen(5)

print(f"Master iniciado em {HOST}:{PORT}")

while True:
    conn, addr = server.accept()

    thread = threading.Thread(target=handle_client, args=(conn, addr))
    thread.start()