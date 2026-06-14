# Sistema Distribuído P2P com Balanceamento Dinâmico de Workers

Projeto desenvolvido para a disciplina **Arquitetura de Sistemas Distribuídos**, implementando uma arquitetura Master-Worker com comunicação P2P entre Masters, redistribuição dinâmica de Workers e monitoramento centralizado.

# Objetivo

Implementar uma fazenda distribuída de processamento composta por Masters e Workers capazes de:

* Distribuir tarefas para Workers.
* Monitorar disponibilidade através de Heartbeats.
* Compartilhar Workers entre Masters quando houver sobrecarga.
* Realizar devolução automática de Workers emprestados.
* Reportar métricas operacionais para um Dashboard Supervisor.

---

# Tecnologias Utilizadas

* Python 3
* Socket TCP
* JSON
* Threads
* SSL/TLS
* Biblioteca padrão do Python

---

# Arquitetura

A solução utiliza uma arquitetura híbrida:

```text
          +----------------+
          |   Supervisor   |
          | Dashboard TLS  |
          +--------+-------+
                   ^
                   |
        performance_report
                   |
     +-------------+-------------+
     |                           |
+----+----+                +-----+----+
| Master A | <--------->   | Master B |
+----+----+    P2P         +-----+----+
     |                           |
     |                           |
+----+----+                +-----+----+
|Worker 1 |                |Worker 2  |
+---------+                +----------+
```

---

# Sprint 1

## Heartbeat

Implementação da comunicação Worker → Master para verificar disponibilidade.

### Mensagem

```json
{
  "TASK": "HEARTBEAT",
  "SERVER_UUID": "A"
}
```

### Resposta

```json
{
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}
```

---

# Sprint 2

## Distribuição de Tarefas

O Master mantém uma fila de tarefas.

Quando um Worker se conecta:

1. Solicita trabalho.
2. Recebe uma tarefa.
3. Processa a tarefa.
4. Retorna STATUS.
5. Recebe ACK.

Fluxo:

```text
Worker -> ALIVE
Master -> QUERY
Worker -> STATUS OK/NOK
Master -> ACK
```

---

# Sprint 3

## Empréstimo Dinâmico de Workers

Quando a fila de um Master ultrapassa sua capacidade:

```text
current_load > capacity
```

o Master solicita ajuda aos vizinhos.

### request_help

```json
{
  "type": "request_help",
  "payload": {
    "workers_needed": 2
  }
}
```

### response_accepted

```json
{
  "type": "response_accepted"
}
```

O Master vizinho redireciona Workers ociosos para auxiliar o Master sobrecarregado.

---

## Devolução Automática

Quando a carga retorna ao normal:

```text
current_load <= release_threshold
```

os Workers emprestados são devolvidos ao Master original.

---

# Sprint 4

## Monitoramento e Dashboard

Cada Master envia periodicamente métricas operacionais ao Supervisor.

### Intervalo

```text
10 segundos
```

### Canal

```text
TLS / SSL
```

### Tipo de Mensagem

```text
performance_report
```

### Exemplo

```json
{
  "server_uuid": "A",
  "role": "master",
  "task": "performance_report",
  "payload_version": "sprint4-monitor",
  "performance": {
    "farm_state": {
      "workers": {
        "total_registered": 2,
        "workers_utilization": 1,
        "workers_alive": 2,
        "workers_idle": 1
      },
      "tasks": {
        "tasks_pending": 5,
        "tasks_completed": 20,
        "tasks_failed": 1
      }
    }
  }
}
```

---

# Execução

## Master A

```bash
python master.py \
  --master-id A \
  --port 5000 \
  --seed-tasks 20
```

## Master B

```bash
python master.py \
  --master-id B \
  --port 5001 \
  --neighbor A=127.0.0.1:5000
```

## Worker 1

```bash
python worker.py \
  --worker-id W1 \
  --master-id A \
  --master-address 127.0.0.1:5000 \
  --command-port 6001
```

## Worker 2

```bash
python worker.py \
  --worker-id W2 \
  --master-id A \
  --master-address 127.0.0.1:5000 \
  --command-port 6002
```

---

# Funcionalidades Implementadas

* [x] Heartbeat Worker → Master
* [x] Distribuição de tarefas
* [x] ACK/NACK de processamento
* [x] Compartilhamento de Workers
* [x] Redirecionamento de Workers
* [x] Devolução automática
* [x] Monitor de carga
* [x] Comunicação Master ↔ Master
* [x] Monitoramento remoto
* [x] Envio de métricas para Dashboard
* [x] Comunicação segura TLS

---

# Resultados

O sistema demonstrou capacidade de:

* Distribuir tarefas entre Workers.
* Detectar saturação de carga.
* Compartilhar recursos entre Masters.
* Recuperar Workers automaticamente.
* Reportar métricas em tempo real para supervisão centralizada.