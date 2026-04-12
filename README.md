## Configuração

### No arquivo `master.py`

Defina o IP da máquina:

```python
iniciar_master("192.168.1.47")
```

---

### No arquivo `worker.py`

Configure:

```python
MEU_IP = "192.168.1.47"

MASTER = ("192.168.1.47", 2003)

WORKERS = [
    ("192.168.1.47", 2003),
]
```

Para múltiplas máquinas, adicione os IPs na lista `WORKERS`.

---

## Execução

### 1. Iniciar o Master

No terminal:

```bash
python master.py
```

Saída esperada:

```plaintext
MASTER ATIVO EM 192.168.1.47:2003
```

---

### 2. Iniciar o Worker

Em outro terminal:

```bash
python worker.py
```

Saída esperada:

```plaintext
Registrando no master: ('192.168.1.47', 2003)
Heartbeat para: ('192.168.1.47', 2003)
```

---

## Funcionamento

* O worker envia heartbeat a cada 3 segundos
* O master responde confirmando que está ativo
* O worker se registra no master ao iniciar
* O master mantém uma lista de workers ativos

---

## Eleição de Novo Master

Se o worker não conseguir se conectar ao master após 4 tentativas:

* Inicia o processo de eleição
* Consulta o espaço livre de disco dos nós
* Escolhe o nó com maior espaço disponível
* Em caso de empate, usa o IP como critério
* O novo master é anunciado para os demais nós
* O nó eleito assume o papel de master

---

## Teste de Failover

1. Inicie o master
2. Inicie o worker
3. Encerre o master (`Ctrl + C`)
4. Observe no worker:

```plaintext
Falha: 4
Iniciando eleição
Novo master: (...)
Este nó virou master
```
