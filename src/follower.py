import Pyro5.api
import random
import time
import threading

REPLICAS = {
    "1": {"id": "p1", "port": 9001},
    "2": {"id": "p2", "port": 9002},
    "3": {"id": "p3", "port": 9003},
    "4": {"id": "p4", "port": 9004},
}

@Pyro5.api.expose
class LogReplicator():
    def __init__(self, my_id):
        self.my_id = my_id
        self.state = "follower"
        self.term = 0
        self.voted_for = None

        self.heartbeat_event = threading.Event()
        self.lock = threading.Lock()

        self.peers = [
            f"PYRO:{data['id']}@localhost:{data['port']}"
            for key, data in REPLICAS.items() if data['id'] != self.my_id
        ]
    
    def start_timeout_thread(self):
        timeout_th = threading.Thread(
            target=self.election_timer_loop,
            name="election_timer_thread",
            daemon=True
        )
        timeout_th.start()
    
    def election_timer_loop(self):
        while True:
            timeout = random.uniform(2.0, 4.0)
            got_heartbeat = self.heartbeat_event.wait(timeout)

            if got_heartbeat:
                self.heartbeat_event.clear()
                continue

            if self.state != 'leader':
                self.start_election()

    def start_election(self):
        with self.lock:
            self.state = "candidate"
            self.term += 1
            self.voted_for = self.my_id
            current_term = self.term
            print(f"\n### Temporizador expirou. \
                  Começando nova eleição para o termo {current_term}) ###")

        votes = 1

        for peer_uri in self.peers:
            try:
                peer = Pyro5.api.Proxy(peer_uri)
                got_vote = peer.request_vote(current_term, self.my_id)
                if got_vote:
                    votes += 1 
            except Exception:
                pass
        
        with self.lock:
            if self.state == 'candidate':
                if votes > len(REPLICAS) / 2:
                    self.state = "leader"
                    print(f"*** LÍDER ELEITO para o termo {current_term} com {votes} votos ***")
                else:
                    self.state = "follower"
                    print(f"Eleição falhou com {votes} votos. Voltando ao estado de seguidor")

    
    def append_entry(self, term, leader_id, entry):
        with self.lock:
            if term >= self.term:
                self.term = term
                self.state = "follower"
                self.heartbeat_event.set()
                return True
            return False
        
    def request_vote(self, term, candidate_id):
        with self.lock:
            if term > self.term:
                self.term = term
                self.state = "follower"
                self.voted_for = None
            
            if term == self.term and self.voted_for is None:
                self.voted_for = candidate_id
                self.heartbeat_event.set()
                print(f"Voto registrado em {candidate_id} para o termo {term}")
                return True
            
            return False

def start_daemon(port, object_id):
    try:
        daemon = Pyro5.api.Daemon(port=int(port))
        replicator_instance = LogReplicator(object_id)
        uri = daemon.register(replicator_instance, objectId=object_id)

        print(f"Objeto Pronto. URI = {uri}")
        return daemon, replicator_instance
    except Exception as e:
        print(f"Erro inicializando o Daemon: {e}")
        return None, None

if __name__ == "__main__":
    replica_id = input("Digite o número da replica (1-4): ").strip()
    if replica_id not in REPLICAS:
        print("Valor inválido. Escolha um número entre 1 e 4!")
        exit()

    my_config = REPLICAS[replica_id]

    print(f"Inicializando {my_config['id']} na porta {my_config['port']}...")
    daemon, log_replicator = start_daemon(my_config["port"], my_config["id"])
    if daemon and log_replicator:
        log_replicator.start_timeout_thread()
        try:
            daemon.requestLoop()
        except KeyboardInterrupt:
            print("Abortando...")
