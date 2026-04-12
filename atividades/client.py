import socket
import json
import time

HOST = '172.31.88.25'
PORT = 2003

payload = {
    "SERVER_UUID": "master-01",
    "TASK": "HEARTBEAT"
}

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.connect((HOST, PORT))

    time.sleep(1)

    mensagem = json.dumps(payload) + "\n"
    s.sendall(mensagem.encode())
    print("HEARTBEAT enviado com sucesso")

    resposta = s.recv(1024).decode().strip()

    if resposta:
        try:
            resposta_json = json.loads(resposta)

            print("Resposta recebida do Master:")
            print(resposta_json)

            if (
                resposta_json.get("SERVER_UUID") == "master-01"
                and resposta_json.get("TASK") == "HEARTBEAT"
                and resposta_json.get("RESPONSE") == "ALIVE"
            ):
                print("Master está ativo.")
            else:
                print("Resposta fora do padrão esperado.")

        except json.JSONDecodeError:
            print("Erro: resposta não é JSON válido.")
    else:
        print("Nenhuma resposta recebida.")

except ConnectionRefusedError:
    print("Erro: não foi possível conectar ao servidor.")

except Exception as e:
    print(f"Erro inesperado: {e}")

finally:
    s.close()
    print("Conexão encerrada.")