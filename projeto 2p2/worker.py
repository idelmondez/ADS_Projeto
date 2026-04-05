import socket
import json
import time

HOST = '192.168.1.12'
PORT = 2003

payload = {
    "SERVER_UUID": "master-01",
    "TASK": "HEARTBEAT"
}

def enviar_heartbeat():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))

        mensagem = json.dumps(payload) + "\n"
        s.sendall(mensagem.encode())

        print("[>] HEARTBEAT enviado")

        resposta = s.recv(1024).decode().strip()

        if resposta:
            resposta_json = json.loads(resposta)

            print("[<] Resposta recebida:")
            print(resposta_json)

            if resposta_json.get("RESPONSE") == "ALIVE":
                print("[OK] Master está ativo\n")
            else:
                print("[ERRO] Resposta inválida\n")
        else:
            print("[ERRO] Sem resposta do servidor\n")

        s.close()

    except Exception as e:
        print(f"[FALHA] Não foi possível conectar: {e}\n")

while True:
    enviar_heartbeat()
    time.sleep(3)