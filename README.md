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

## Make file commands - step by step
* To clean persistant files - **make clean**
* To create .proto file - **make proto**
* start server - using above commands 
* To setup connection among server - **make setup**
* To start election among servers - **make election**
* To start client - **make client**