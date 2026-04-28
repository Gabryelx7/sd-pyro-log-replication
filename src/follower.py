import Pyro5.api
import time

@Pyro5.api.expose
class DummyClass():
    def __init__(self):
        pass
    
    @Pyro5.api.oneway
    def dummy_method(self):
        time.sleep(1)
        print("Método de teste executado!")

def start_daemon(port, object_id):
    try:
        daemon = Pyro5.api.Daemon(port=int(port))
        uri = daemon.register(DummyClass, objectId=object_id)

        print(f"Objeto Pronto. URI = {uri}")
        return daemon
    except Exception as e:
        print(f"Erro inicializando o Daemon: {e}")
        return

if __name__ == "__main__":
    port = input("Digite uma porta para o objeto: ").strip()
    object_id = input("Digite um ID para o objeto: ").strip()

    daemon = start_daemon(port, object_id)
    if daemon:
        try:
            daemon.requestLoop()
        except Exception as e:
            print(f"Erro: {e}")
