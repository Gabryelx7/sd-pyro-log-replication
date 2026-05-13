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
        self.commit_index = -1
        self.next_index = {}
        self.match_index = {}

        self.heartbeat_event = threading.Event()
        self.lock = threading.Lock()
        self.commit_cond = threading.Condition(self.lock)

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
                with Pyro5.api.Proxy(peer_uri) as peer:
                    peer._pyroTimeout = 0.5
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

                    for peer in self.peers:
                        self.next_index[peer] = len(self.log)
                        self.match_index[peer] = -1

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
    
    def execute_command(self, command):
        if self.state != "leader":
            return False
        
        with self.commit_cond:
            new_entry = {"term": self.term, "command": command}
            self.log.append(new_entry)
            target_idx = len(self.log) - 1
            print(f"-> Líder adicionou comando: '{command}'. Esperando resposta da maioria...")

            while self.commit_index < target_idx and self.state == "leader":
                self.commit_cond.wait(timeout=0.5)
            
            if self.state != "leader":
                print("Processo perdeu a liderança enquando esperava pelo consenso.")
                return False
        
            return True

    def heartbeat_loop(self):
        while self.state == "leader":
            for peer_uri in self.peers:
                with self.lock:
                    next_idx = self.next_index[peer_uri]
                    prev_log_idx = next_idx - 1
                    prev_log_term = self.log[prev_log_idx]['term'] if prev_log_idx >= 0 else 0

                    entry_to_send = self.log[next_idx] if next_idx < len(self.log) else None
                    current_commit = self.commit_index
                
                try:
                    with Pyro5.api.Proxy(peer_uri) as peer:
                        peer._pyroTimeout = 0.5
                        success, follower_term = peer.append_entry(
                            self.term,
                            self.my_id,
                            prev_log_idx,
                            prev_log_term,
                            entry_to_send,
                            current_commit
                        )

                        with self.lock:
                            if self.state != "leader":
                                break
                            
                            if success:
                                if entry_to_send:
                                    self.next_index[peer_uri] = next_idx + 1
                                    self.match_index[peer_uri] = next_idx
                            else:
                                if follower_term > self.term:
                                    self.state = "follower"
                                    self.term = follower_term
                                else:
                                    self.next_index[peer_uri] = max(0, next_idx - 1)

                except Exception:
                    pass
            
            with self.commit_cond:
                if self.state == "leader":
                    match_indices = [len(self.log) - 1] + list(self.match_index.values())
                    match_indices.sort(reverse=True)
                    majority_index = match_indices[len(REPLICAS) // 2]

                    if majority_index > self.commit_index and self.log[majority_index]['term'] == self.term:
                        self.commit_index = majority_index
                        print(f"-> Líder atingiu a maioria. Índice de commit atual: {self.commit_index}.")
                        self.commit_cond.notify_all()
                
            time.sleep(0.5)

    def append_entry(self, term, leader_id, prev_log_index, prev_log_term, entry, leader_commit):
        with self.lock:
            if term < self.term:
                return False, self.term

            if self.state != "follower" or self.term != term:
                print(f"Reconhecendo novo líder {leader_id} para o termo {term}")
            self.term = term
            self.state = "follower"
            self.voted_for = None
            self.heartbeat_event.set()

            if prev_log_index >= len(self.log):
                return False, self.term
            
            if prev_log_index >= 0 and self.log[prev_log_index]['term'] != prev_log_term:
                return False, self.term
            
            if entry:
                new_index = prev_log_index + 1

                if new_index < len(self.log):
                    if self.log[new_index]['term'] != entry['term']:
                        print(f"    Conflito de log no índice {new_index}. Substituindo a entrada.")
                        self.log = self.log[:new_index]
                        self.log.append(entry)
                    
                else:
                    self.log.append(entry)
                    print(f"    Entrada adicionada no índice {new_index}. Tamanho do log: {len(self.log)}")

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                
            return True, self.term
        
    def request_vote(self, term, candidate_id):
        with self.lock:
            if term > self.term:
                self.term = term
                self.state = "follower"
                self.voted_for = None
            
            if term == self.term and (self.voted_for is None or self.voted_for == candidate_id):
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
