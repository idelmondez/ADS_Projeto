# Agent instructions

This repository is a Python master/worker prototype with heartbeat and election failover.

Quick map
- master.py: master node server and registry
- worker.py: worker loop, election, and local master bootstrap
- test_election.py: threaded test (1 master + 1 worker)
- test_multi_workers.py: multi-process integration test'

Docs
- See README.md for full run/test instructions and environment variables.

Common commands
- python master.py
- python worker.py
- python test_election.py
- python test_multi_workers.py

Notes
- Default ports: master 2003, worker control 2103
- MASTER_HOST and MEU_IP default to auto-detected local IP via UDP socket
