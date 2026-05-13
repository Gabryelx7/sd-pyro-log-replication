import Pyro5.api
import time

def get_leader():
    try:
        ns = Pyro5.api.locate_ns()
        leader_uri = ns.lookup("Leader")
        return Pyro5.api.Proxy(leader_uri)
    except Exception:
        return None

if __name__ == "__main__":
    print("--- Processo Cliente Iniciado ---")
    print("Conectando ao Name Server para encontrar o líder...")
    
    while True:
        try:
            command = input("\Digite um comando (ou 'sair'): ").strip()
            if command.lower() == 'sair':
                break
            if not command:
                continue

            leader = get_leader()
            if not leader:
                print("Erro: Não foi possível encontrar o líder no Name Server. Tentando novamente...")
                time.sleep(2)
                continue

            success = leader.execute_command(command)
            
            if success:
                print(f"Comando recebido pelo líder!")
            else:
                print(f"Comando rejeitado pelo líder! Tentando novamente...")
                
        except Exception as e:
            print(f"Conexão perdida. Erro: {e}")
            time.sleep(2)
            