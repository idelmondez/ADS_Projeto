import threading
import time

import master
import worker


def start_master(host='127.0.0.1', port=2003):
    stop = threading.Event()
    t = threading.Thread(target=master.iniciar_master, args=(host, port, stop), daemon=True)
    t.start()
    return t, stop


def start_worker(meu_ip='127.0.0.1', master_addr=('127.0.0.1', 2003), worker_uuid='W-1', control_port=2103):
    # configure worker module
    worker.MEU_IP = meu_ip
    worker.MASTER = master_addr
    worker.WORKER_UUID = worker_uuid
    worker.CONTROL_PORT = control_port

    # start control listener
    ctl = threading.Thread(target=worker.controle_listener, daemon=True)
    ctl.start()

    # start main loop
    main = threading.Thread(target=worker.main_loop, daemon=True)
    main.start()

    return ctl, main


if __name__ == '__main__':
    print('Starting master and worker for test...')
    m_thread, m_stop = start_master()
    ctl, main = start_worker()

    # let them run and exchange heartbeat/registers
    time.sleep(8)

    print('Stopping master to simulate failure...')
    m_stop.set()

    # wait long enough for worker to detect failure and run election
    time.sleep(15)

    print('Test finished. Check logs above for election and master startup.')
