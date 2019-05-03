#!usr/bin/python3

import sys
import socket
from constants import *

sys.path.append('/home/dipika_patil_linux/Downloads/protobuf-3.7.0/python')
import KeyValueClusterStore_pb2
from google.protobuf.internal.encoder import _VarintEncoder
from google.protobuf.internal.decoder import _DecodeVarint


# Method to encode an int as a protobuf varint
def encode_varint(value):
    """ Encode an int as a protobuf varint """
    data = []
    _VarintEncoder()(data.append, value, False)
    return b''.join(data)


# Method to decode a protobuf varint to an int
def decode_varint(data):
    """ Decode a protobuf varint to an int """
    return _DecodeVarint(data, 0)[0]


# Starting point for Key-Value client
if __name__ == "__main__":
    # Validating command line arguments
    if len(sys.argv) != 1:
        print("ERROR : Invalid # of command line arguments. Expected 1 arguments.")
        sys.exit(1)

    print("\nConnectClient request sent to leader. Waiting for transaction_id...", file=sys.stderr)

    # Request user for specific operation (get/put)
    l_request_id = 0
    cc_transaction_id = 0
    while True:
        cc_transaction_id += 1
        print("\n------------------------------------------------------------------------------------------------------------------------")
        # Local variables
        leader_ip = input("Enter leader ip ")
        leader_port = int(input("Enter leader port "))
        # cc_transaction_id = ""

        u_req_choice = input("Enter request type (GET - 1 or PUT - 2) --> ")
        u_req_type = ""
        if u_req_choice == "1":
            u_req_type = "get"
        elif u_req_choice == "2":
            u_req_type = "put"
        else:
            u_req_type = "NONE"
        if u_req_type.lower() != "get" and u_req_type.lower() != "put":
            print("INVALID REQUEST TYPE (Request types 'get' or 'put' are acceptable)....Try Again !!", file=sys.stderr)
            continue

        # Create socket IPv4 and TCP
        try:
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except:
            print("ERROR : Socket creation failed.")
            sys.exit(1)

        # Connect coordinator socket to server using 3 way handshake
        try:
            clientSocket.connect((leader_ip, leader_port))
        except:
            print("ERROR : Connection to leader failed")
            continue

        if u_req_type.lower() == "get":
            u_req_type = "get"
            u_key_1 = input("Enter Key a number --> ")
            while not u_key_1.isdigit():
                print("ERROR : key is not number.")
                u_key_1 = input("Enter Key a number --> ")

            u_key = int(u_key_1)

            # Create KeyValueMessage object and wrap ClientRequest object inside it
            kv_message_instance = KeyValueClusterStore_pb2.KeyValueMessage()
            kv_message_instance.client_request.request_type = u_req_type
            kv_message_instance.client_request.transId = cc_transaction_id
            kv_message_instance.client_request.get_request.key = int(u_key)
            kv_message_instance.client_request.request_type = "GET"

            # Send KeyValueMessage to coordinator socket
            data = kv_message_instance.SerializeToString()
            size = encode_varint(len(data))
            clientSocket.sendall(size + data)

            print("GET request sent to leader for key ", u_key, " | Transaction_id = (",
                  cc_transaction_id,"). Waiting for reply...", file=sys.stderr)

            # Identify message type by parsing coordinator socket
            kv_message_response = KeyValueClusterStore_pb2.KeyValueMessage()
            data = clientSocket.recv(1)
            size = decode_varint(data)
            kv_message_response.ParseFromString(clientSocket.recv(size))

            if kv_message_response.WhichOneof('key_value_message') == 'leader_response':
                res_key = kv_message_response.leader_response.key
                res_value = kv_message_response.leader_response.value.strip()
                res_txid = kv_message_response.leader_response.transId
                res_msg = kv_message_response.leader_response.message
                if res_msg == "SUCCESS":
                    if DEBUG_STDERR:
                        print("Response received from leader for GET request | TxID --> ", res_txid, file=sys.stderr)
                    print("Key-Value Pair returned by leader --> ", (res_key, res_value), file=sys.stderr)
                else:
                    print("KEY NOT FOUND at cluster/leader for GET request key ",
                          res_key, "| TxID --> ", res_txid, file=sys.stderr)
        else:
            u_req_type = "put"
            u_key_1 = input("Enter Key a number --> ")
            while not u_key_1.isdigit():
                print("ERROR : key is not number.")
                u_key_1 = input("Enter Key a number --> ")

            u_key = int(u_key_1)
            u_value = input("Enter Value --> ")

            # Create KeyValueMessage object and wrap ClientRequest object inside it
            kv_message_instance = KeyValueClusterStore_pb2.KeyValueMessage()
            kv_message_instance.client_request.request_type = u_req_type
            kv_message_instance.client_request.transId = cc_transaction_id
            kv_message_instance.client_request.put_request.key = int(u_key)
            kv_message_instance.client_request.put_request.value = u_value
            kv_message_instance.client_request.request_type = "PUT"
            kv_message_instance.client_request.clientIp = socket.gethostbyname(socket.gethostname())
            kv_message_instance.client_request.clientPort = clientSocket.getsockname()[1]

            print("clientIP -->", kv_message_instance.client_request.clientIp, "clientPort -->", kv_message_instance.client_request.clientPort, file=sys.stderr)

            # Send KeyValueMessage to coordinator socket
            data = kv_message_instance.SerializeToString()
            size = encode_varint(len(data))
            clientSocket.sendall(size + data)
            print("PUT request sent to leader for key ", u_key, " and value ", u_value , " | Transaction_id = (",
                  cc_transaction_id, "). Waiting for reply...", file=sys.stderr)

            # Identify message type by parsing coordinator socket
            kv_message_response = KeyValueClusterStore_pb2.KeyValueMessage()
            data = clientSocket.recv(1)
            size = decode_varint(data)
            kv_message_response.ParseFromString(clientSocket.recv(size))

            if DEBUG_STDERR:
                print("Response received from cluster/leader for PUT request.", file=sys.stderr)

            if kv_message_response.WhichOneof('key_value_message') == 'leader_response':
                res_key = kv_message_response.leader_response.key
                res_value = kv_message_response.leader_response.value
                res_msg = kv_message_response.leader_response.message
                if res_msg == "SUCCESS":
                    print((res_key, res_value), " pair written at coordinator side successfully.", file=sys.stderr)
                else:
                    print("Something is wrong, PUT request code should not reach here", file=sys.stderr)

        clientSocket.close()