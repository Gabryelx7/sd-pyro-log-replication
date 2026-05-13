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
    def __init__(self, my_config):
        self.my_id = my_config["id"]
        self.my_uri = f"PYRO:{self.my_id}@localhost:{my_config['port']}"
        self.state = "follower"
        self.term = 0
        self.voted_for = None
        
        self.log = []

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

            if self.state != "leader":
                self.start_election()

    def start_election(self):
        with self.lock:
            self.state = "candidate"
            self.term += 1
            self.voted_for = self.my_id
            current_term = self.term
            print(f"\n### Temporizador expirou. Começando nova eleição para o termo {current_term}) ###")

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

                    self.register_leader_ns()
                    threading.Thread(target=self.heartbeat_loop, daemon=True).start()
                else:
                    self.state = "follower"
                    print(f"Eleição falhou com {votes} votos. Voltando ao estado de seguidor")
    
    def register_leader_ns(self):
        try:
            ns = Pyro5.api.locate_ns()
            ns.register("Leader", self.my_uri)
            print("Registro de líder realizado com sucesso no Name Server.")
        except Exception as e:
            print(f"Não foi possível conectar ao Name Server. Erro: {e}")

    def heartbeat_loop(self):
        while self.state == "leader":
            current_log = list(self.log)

            for peer_uri in self.peers:
                try:
                    peer = Pyro5.api.Proxy(peer_uri)
                    peer.append_entry(self.term, self.my_id, current_log)
                except Exception:
                    pass
            time.sleep(1.0)
    
    def execute_command(self, command):
        if self.state != "leader":
            return False
        
        with self.lock:
            new_entry = {"term": self.term, "command": command}
            self.log.append(new_entry)
            print(f"-> Líder adicionou comando: '{command}' no índice {len(self.log)-1}")
        
        return True

    def append_entry(self, term, leader_id, entries):
        with self.lock:
            if term < self.term:
                return False
            
            if term >= self.term:
                if self.state != "follower" or self.term != term:
                    print(f"Reconhecendo novo líder {leader_id} para o termo {term}")
                self.term = term
                self.state = "follower"
                self.heartbeat_event.set()
            
            if len(entries) > len(self.log):
                new_entries_count = len(entries) - len(self.log)
                self.log = entries
                print(f"{new_entries_count} entradas novas foram replicadas. Log agora tem tamanho {len(self.log)}")
                print(f"    Última entrada no log: {self.log[-1]}")
                
            return True
        
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

def start_daemon(my_config):
    try:
        daemon = Pyro5.api.Daemon(port=int(my_config["port"]))
        replicator_instance = LogReplicator(my_config)
        uri = daemon.register(replicator_instance, objectId=my_config["id"])

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
    daemon, log_replicator = start_daemon(my_config)
    if daemon and log_replicator:
        log_replicator.start_timeout_thread()
        try:
            daemon.requestLoop()
        except KeyboardInterrupt:
            print("Abortando...")
