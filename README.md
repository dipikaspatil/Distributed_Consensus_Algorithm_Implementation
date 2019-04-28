## Commands
* protoc --python_out=./ KeyValueClusterStore.proto 
* python3 server.py CNode0 9090
* python3 server.py CNode1 9091
* python3 server.py CNode2 9092
* python3 server.py CNode3 9093
* python3 server.py CNode4 9094
* python3 adminServer.py setup_connection list_replicas.txt 
* python3 adminServer.py start_election list_replicas.txt
* python3 client.py
