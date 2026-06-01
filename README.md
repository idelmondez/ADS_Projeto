# Projeto 2P2 — Atualizações e instruções

Este repositório implementa um protótipo simples de master/workers com suporte a:
- Heartbeat e ciclo de tarefas (QUERY / NO_TASK / STATUS)
- Failover por eleição entre workers (DISK / NEW_MASTER)
- Mensagens Master-to-Master (`type` + `request_id` + `payload`)
- Canal de controle do worker para `command_redirect` / `command_release`

Resumo das mudanças recentes
- Detector automático de IP local quando `MASTER_HOST` / `MEU_IP` não forem fornecidos.
- Workers podem ser temporariamente "emprestados" entre masters (register_temporary_worker).
- Melhor tratamento de mensagens de controle e robustez em listeners.
- A descoberta por broadcast assume que os nós estão na mesma rede cabeada/LAN.

Arquivos principais
- [master.py](master.py)
- [worker.py](worker.py)
- [test_multi_workers.py](test_multi_workers.py)
- [test_election.py](test_election.py)

Configuração rápida

- Variáveis de ambiente úteis:
  - `MASTER_HOST` / `MASTER_PORT`: força o endereço do master (ex.: `MASTER_HOST=10.0.0.5`).
  - `MEU_IP`: força o IP do worker; se ausente, o código tenta detectar o IP local automaticamente.
  - `CONTROL_PORT`: porta do canal de controle do worker (padrão 2103).
  - `WORKER_UUID`: identificador do worker.

- Valores no código (padrões):
  - `MASTER = None` em `worker.py` (quando `None` usa `MEU_IP:PORT` ou variáveis de ambiente).
  - `HOST = None` em `master.py` (quando `None` o host é detectado automaticamente).

Execução local

1) Start manual (master separado):

```
python master.py
```

2) Start de um worker (opcionalmente via env vars para testes):

```
MEU_IP=192.168.1.10 WORKER_UUID=W-1 CONTROL_PORT=2103 MASTER_HOST=192.168.1.5 MASTER_PORT=2003 python worker.py
```

(Em Windows PowerShell, use `setx` ou prefira export via script/variáveis do processo.)

Testes locais

- Teste de eleição simples (inicia master + 1 worker em threads):

```
python test_election.py
```

- Teste de integração com múltiplos workers (inicia processos separados; útil para validar failover):

```
python test_multi_workers.py
```

Observações

- Se `MASTER_HOST` / `MEU_IP` não forem fornecidos, o código tenta determinar o IP local usando a técnica de criar um socket UDP e consultar `getsockname()` — esse método escolhe a interface de saída padrão e funciona na maioria dos cenários de rede. Em ambientes com múltiplas interfaces ou VPNs, forneça explicitamente o IP desejado via variáveis de ambiente.

- A descoberta entre masters por broadcast usa a mesma LAN cabeada e a porta comum `10000`. Em redes com VLANs, sub-redes distintas ou Wi-Fi isolado, o broadcast pode não alcançar todos os nós.

- Para testes em rede real, confira firewall e regras de NAT para permitir conexões TCP na porta `2003` (masters) e nas portas `CONTROL_PORT` (workers). A porta `10000` deve ficar liberada para a comunicação entre masters na mesma LAN.

- Para desenvolvimento iterativo, recomendo executar `test_multi_workers.py` localmente para validar comportamento de eleição e failover antes de distribuir entre máquinas.

---
Atualizado: 2026-05-18 — documentação alinhada às mudanças de detecção automática de IP e testes.
