import Pyro5.api
import random
import time
import threading

# não precisa lidar com falhas nas replicas

@Pyro5.api.expose
class LogReplicator():
    def __init__(self):
        self.state = "follower"

        self.heartbeat_event = threading.Event()
        self.lock = threading.Lock()
    
    def start_timeout_thread(self):
        timeout_th = threading.Thread(
            target=self.election_timer_loop,
            name="election_timer_thread",
            daemon=True
        )
        timeout_th.start()
    
    def election_timer_loop(self):
        while True:
            timeout = random.randint(150, 300) / 100

            got_heartbeat = self.heartbeat_event.wait(timeout)

            if got_heartbeat:
                self.heartbeat_event.clear()
                continue

            if self.state != 'leader':
                with self.lock:
                    pass    # Implementar eleição
            
    
    def append_entry(self, entry):
        if not entry: # heartbeat
            self.heartbeat_event.set()

def start_daemon(port, object_id):
    try:
        daemon = Pyro5.api.Daemon(port=int(port))
        uri = daemon.register(LogReplicator, objectId=object_id)

        print(f"Objeto Pronto. URI = {uri}")
        return daemon
    except Exception as e:
        print(f"Erro inicializando o Daemon: {e}")
        return

if __name__ == "__main__":
    port = input("Digite uma porta para o objeto: ").strip()
    object_id = input("Digite um ID para o objeto: ").strip()

    log_replicator = LogReplicator()
    log_replicator.start_timeout_thread()
    
    daemon = start_daemon(port, object_id)
    if daemon:
        try:
            daemon.requestLoop()
        except Exception as e:
            print(f"Erro: {e}")
