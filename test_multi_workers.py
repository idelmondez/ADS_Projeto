
import os
import subprocess
import threading
import time

PY = 'c:/python313/python.exe'
MASTER_HOST = '127.0.0.1'
MASTER_PORT = '2003'

procs = []
reader_threads = []


def start_reader(name, proc):
    def _reader():
        for line in proc.stdout:
            print(f'[{name}] {line.rstrip()}')

    thread = threading.Thread(target=_reader, daemon=True)
    thread.start()
    reader_threads.append(thread)

# Start master process
master_env = os.environ.copy()
master_env['PYTHONUNBUFFERED'] = '1'
master_env['MASTER_HOST'] = MASTER_HOST
master_env['MASTER_PORT'] = MASTER_PORT
master_p = subprocess.Popen([PY, '-u', 'master.py'], cwd='.', stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=master_env)
procs.append(('master', master_p))
start_reader('master', master_p)
print('Master started, PID', master_p.pid)

# give master time to start
time.sleep(2)

# Start 3 workers with distinct CONTROL_PORT and WORKER_UUID
for i in range(3):
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    env['MEU_IP'] = f'127.0.0.1'
    env['WORKER_UUID'] = f'W-{i+1}'
    env['CONTROL_PORT'] = str(2103 + i)
    env['MASTER_HOST'] = MASTER_HOST
    env['MASTER_PORT'] = MASTER_PORT

    p = subprocess.Popen([PY, '-u', 'worker.py'], cwd='.', stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    procs.append((f'worker-{i+1}', p))
    start_reader(f'worker-{i+1}', p)
    print(f'Worker-{i+1} started, PID {p.pid}')
    time.sleep(1)

# Let them run and interact
time.sleep(8)

print('\n-- Stopping master to simulate failure --')
# stop master
for name, p in procs:
    if name == 'master':
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
        print('Master stopped')
        break

time.sleep(20)

print('\n-- Shutting down remaining processes --')
for name, p in procs:
    if p.poll() is None:
        try:
            p.terminate()
            p.wait(timeout=2)
        except Exception:
            p.kill()
        print('Stopped', name)

time.sleep(1)

print('Test complete')
