proto:
	protoc --python_out=./ KeyValueClusterStore.proto

setup:
	python3 adminServer.py setup_connection list_replicas.txt

election:
	python3 adminServer.py start_election list_replicas.txt

client:
	python3 client.py

clean:
	rm -rf CNode*_file.txt CNode*_log_file.log KeyValueClusterStore_pb2.py
