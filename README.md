## PyRo Raft: Replicação de Logs

Este projeto é uma implementação simplificada do algoritmo de consenso Raft, construída em Python usando Pyro5 para Chamadas de Procedimento Remoto (RPC). Ele demonstra a eleição de líder, a transmissão de heartbeats e a replicação de logs por consenso majoritário em nós distribuídos.

### Pré-requisitos

Antes de executar o projeto, certifique-se de ter o Python 3 instalado, juntamente com a biblioteca Pyro5.

Você pode instalar a dependência necessária via pip:
``` Bash
pip install Pyro5
```

### Arquivos do Projeto

node.py: Contém a lógica do nó Raft (estados de seguidor, candidato e líder) e lida com a replicação e eleição de logs.

client.py: Uma interface de linha de comando simples que descobre o Líder atual através do Servidor de Nomes Pyro e envia entradas de log.

### Como Executar o Cluster

Para simular corretamente o ambiente distribuído, você precisará abrir várias janelas de terminal.

#### Passo 1: Inicie o Servidor de Nomes Pyro

O Servidor de Nomes funciona como um diretório, permitindo que o cliente encontre quem estiver atualmente eleito como "Líder".

Abra um terminal e execute:
```Bash
pyro5-ns
```
(Deixe este terminal aberto em segundo plano).

#### Passo 2: Inicie os Nós Raft

O cluster está configurado para suportar 4 nós em execução no localhost (portas 9001 a 9004).

Abra 4 janelas de terminal separadas. Em cada janela, execute o script do nó e atribua a ele um ID exclusivo (1, 2, 3 ou 4):

**Terminal 2:**
```Bash
python src/node.py
# Digite o número do nó: 1
```

**Terminal 3:**
```Bash
python src/node.py
# Digite o número do nó: 2
```

**Terminal 4:**
```Bash
python src/node.py
# Digite o número do nó: 3
```

**Terminal 5:**
```Bash
python src/node.py
# Digite o número do nó: 4
```

Observação: Assim que a maioria dos nós (3 de 4) estiver online, a eleição será bem-sucedida. Um nó se declarará Líder e se registrará no Servidor de Nomes. Os outros se tornarão seguidores.

#### Passo 3: Inicie o Cliente e Envie Comandos

Abra uma sexta janela de terminal para executar o cliente.

```Bash
python client.py
```

Digite qualquer comando e pressione Enter. O cliente encaminhará o comando para o Líder. O Líder o transmitirá para os seguidores, aguardará a confirmação da maioria, confirmará o log e, finalmente, retornará uma mensagem de "Sucesso" para o cliente.

### Testando Falhas

O Raft foi projetado para ser tolerante a falhas. Você pode testar isso em tempo real:

**Falha do Líder:** Vá para o terminal do Líder atual e encerre o processo (Ctrl+C).

**Reeleição:** Observe os terminais dos seguidores restantes. Após 2 a 4 segundos, seus temporizadores de eleição expirarão e eles elegerão automaticamente um novo Líder.

**Resiliência do Cliente:** Volte para o terminal do cliente e digite um novo comando. O cliente descobrirá automaticamente o novo Líder no Servidor de Nomes e adicionará o comando ao log distribuído sem problemas.