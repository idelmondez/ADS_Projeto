## Projeto 2P2 - Atualizacoes Implementadas

Este projeto foi atualizado para alinhar com o documento `Projeto 2P2.pdf`.

## O que foi atualizado

1. Sprint 1 (Heartbeat)
- Worker envia `{"SERVER_UUID": "...", "TASK": "HEARTBEAT"}`.
- Master responde `{"SERVER_UUID": "...", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}`.
- Todas as mensagens JSON seguem delimitador `\n` em TCP.

1. Sprint 2 (Ciclo de tarefas)
- Worker se apresenta com `WORKER=ALIVE` e `WORKER_UUID`.
- Master entrega `QUERY` com `USER` ou `NO_TASK` quando fila vazia.
- Worker processa e reporta `STATUS` (`OK` ou `NOK`) com `TASK=QUERY` e `WORKER_UUID`.
- Master responde `ACK`.

1. Sprint 3 (Master-to-Master)
- Master suporta mensagens por `type` + `request_id` + `payload`.
- Implementado receptor para:
    - `request_help`
    - `register_temporary_worker`
    - `notify_worker_returned`
- Worker suporta canal de controle para:
    - `command_redirect`
    - `command_release`

## Configuracao

Ajuste os enderecos em `master.py` e `worker.py`:

```python
# master.py
SERVER_UUID = "Master_A"
HOST = "10.62.134.143"
PORT = 2003
NEIGHBORS = [
    ("Master_B", "192.168.1.48", 2003),
]
```

```python
# worker.py
MEU_IP = "10.62.134.143"
PORT = 2003
CONTROL_PORT = 2103
WORKER_UUID = "W-123"
MASTER = ("10.62.134.143", 2003)
MASTER_UUID = "Master_A"
```

## Execucao

1. Inicie o Master:

```bash
python master.py
```

1. Inicie o Worker em outro terminal:

```bash
python worker.py
```

## Comportamento esperado

- Heartbeat periodico do worker para o master.
- Worker solicita tarefa com `ALIVE` e recebe `QUERY` ou `NO_TASK`.
- Quando recebe `QUERY`, processa e reporta `STATUS`; em seguida recebe `ACK`.
- Master loga origem do worker (local ou emprestado).
- Estrutura de negociacao Master-to-Master pronta para interoperabilidade via `request_id`.

## Compatibilidade

- Fluxo legado de eleicao/failover (`DISK`, `NEW_MASTER`) foi mantido.
- Mensagens com `type` desconhecido sao logadas e ignoradas sem derrubar processo.

## Implantação em múltiplas máquinas

Instruções para rodar o sistema em uma rede com vários computadores (cada um com seu IP). Siga estes passos em cada máquina antes de iniciar os processos.

- 1) Escolha os papéis iniciais
    - Em cada máquina que será potencial `Master`, edite [master.py](master.py) definindo `SERVER_UUID` e `HOST` para o IP dessa máquina. Exemplo:

```python
# master.py (exemplo)
SERVER_UUID = "Master_A"        # identifique unicamente cada master
HOST = "10.62.134.143"         # IP desta máquina
PORT = 2003                      # mesma porta em toda a rede
```

    - Em cada máquina que será `Worker`, edite [worker.py](worker.py) definindo `MEU_IP` e `WORKER_UUID` e apontando `MASTER` para o master inicial conhecido (ip,port):

```python
# worker.py (exemplo)
MEU_IP = "10.62.134.144"        # IP desta máquina (worker)
WORKER_UUID = "W-001"           # identifique unicamente cada worker
MASTER = ("10.62.134.143", 2003) # ip:porta do master inicial
CONTROL_PORT = 2103              # porta usada para comandos redirect/release
```

- 2) Não liste IPs individuais de Workers em arquivos de configuração
    - Não é necessário nem recomendado codificar os IPs de cada worker no código. O `master` descobrirá os workers que se conectarem (e retornará a lista via `HEARTBEAT`/`REGISTER`). Os workers atualizarão `known_workers` dinamicamente a partir da resposta do `master`.

- 3) Redes e portas
    - Use a mesma `PORT` para todos os masters e abra essa porta no firewall.
    - Abra também a `CONTROL_PORT` nas máquinas que rodarem workers para aceitar `command_redirect` e `command_release`.

- 4) Neighbors (masters vizinhos)
    - Em [master.py](master.py) o campo `NEIGHBORS` deve conter outros *masters* conhecidos (master_id, ip, port). Exemplo:

```python
NEIGHBORS = [ ("Master_B", "10.62.134.144", 2003), ... ]
```

    - Esses endpoints são usados para negociar (`request_help`). Se sua topologia for dinâmica, mantenha uma forma de atualizá-los (DNS, arquivo de configuração central, ou serviço de descoberta).

- 5) Ordem de inicialização
    - Inicialmente: escolha um master e inicie `master.py` nessa máquina (ou inicie `worker.py` em modo que possa se tornar master automaticamente). Depois inicie `worker.py` nas demais máquinas.
    - Nota: o código atual suporta iniciar `master` em thread a partir do `worker` — um worker eleito passa a rodar um `master` local automaticamente.

- 6) Verificações e manutenção
    - Verifique conectividade TCP entre máquinas (`telnet ip port` / `nc -vz ip port`).
    - Garanta que cada `SERVER_UUID` e `WORKER_UUID` sejam únicos na rede.
    - Se um master cair, os workers executarão eleição e um worker pode virar master; o master anterior poderá retornar como worker ao reestabelecer-se.

- 7) Executar
    - Em cada máquina (após ajustes):

```bash
python master.py   # se for iniciar master manualmente
python worker.py   # iniciar worker (pode também iniciar master local quando eleito)
```

Se desejar, mantenha um arquivo central com a lista de `NEIGHBORS` para facilitar administração e atualize o `README.md` ou scripts de implantação para refletir o workflow de sua infraestrutura.

## Teste rápido (local)

O teste abaixo foi executado localmente usando `127.0.0.1` como endpoint.

- Comandos usados:

```bash
python master.py
python worker.py
```

- Trechos do log observado (Master):

```
[2026-05-11 19:30:11] MASTER ATIVO EM 127.0.0.1:2003 (Master_A)
[2026-05-11 19:30:30] Conexao de ('127.0.0.1', 64776) mensagem={'WORKER': 'ALIVE', 'WORKER_UUID': 'W-123', 'CONTROL_ADDRESS': '127.0.0.1:2103'}
[2026-05-11 19:30:31] Status recebido: worker=W-123 origem=local status=OK
[2026-05-11 19:30:34] Saturacao detectada: load=14 capacity=10
```

- Trechos do log observado (Worker):

```
[WORKER W-123] Registrando no master (legacy): ('127.0.0.1', 2003)
[WORKER W-123] Canal de controle ouvindo em 127.0.0.1:2103
[WORKER W-123] Task para USER=Ana concluida com OK. ACK={'STATUS': 'ACK', 'WORKER_UUID': 'W-123'}
```

Observações: o Master enfileirou tarefas (seed) para simular carga e o Worker recebeu e processou `QUERY` geradas, reportando `STATUS` e recebendo `ACK` do Master. A negociação com `Master_B` expirou por timeout no teste local (esperado, pois não havia vizinho acessível).
