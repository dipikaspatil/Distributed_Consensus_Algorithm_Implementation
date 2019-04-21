#!usr/bin/python3

import sys
import socket
from os import path
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


# Class to represent Other Cluster Information
class ClusterInfo:
    """Class to represent Replica Information"""

    def __init__(self, cNameIn, cIpIn, cPortIn):
        self.clusterName = cNameIn
        self.clusterIpAddress = cIpIn
        self.clusterPortNumber = int(cPortIn)


# Method to create new replica info
def createClusterInfo(file_lineIn):
    clusterName, clusterIpAddress, clusterPortNumber = file_lineIn.split(" ")
    return ClusterInfo(clusterName.strip(), clusterIpAddress.strip(), clusterPortNumber.strip())


# Method to add cluster in protobuf object
def includeAllClusters(newClusterIn, cluster):
    newClusterIn.name = cluster.clusterName
    newClusterIn.ip = cluster.clusterIpAddress
    newClusterIn.port = cluster.clusterPortNumber


# Starting point for Setup Replica Connections
if __name__ == "__main__":
    # Validating command line arguments
    if len(sys.argv) != 3:
        print("ERROR : Invalid # of command line arguments. Expected 3 arguments.")
        sys.exit(1)

    # Local variables
    requestType = sys.argv[1]
    clusterFilename = sys.argv[2]

    # File validation
    if not (path.exists(clusterFilename) and path.isfile(clusterFilename)):
        print("ERROR : Invalid file in argument.")
        sys.exit(1)

    # Open file, sort using clusterName and store in list of ClusterInfo Object
    clusterInfoList = sorted([createClusterInfo(l[:-1]) for l in open(clusterFilename)], key=lambda x: x.clusterName)

    for clusterVar in clusterInfoList:
        # Create client socket IPv4 and TCP
        try:
            clusterSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except:
            print("ERROR : Socket creation failed.")
            sys.exit(1)

        # Connect client socket to server using 3 way handshake
        clusterSocket.connect((clusterVar.clusterIpAddress, int(clusterVar.clusterPortNumber)))

        # Create KeyValueMessage object and wrap setup_connection object inside it
        kv_message_instance = KeyValueClusterStore_pb2.KeyValueMessage()

        if requestType == "setup_connection":
            for cluster in clusterInfoList:
                includeAllClusters(kv_message_instance.setup_connection.all_clusters.add(), cluster)
        elif requestType == "start_election":
            kv_message_instance.start_election.src = "Admin"
            kv_message_instance.start_election.term = 1
        else :
            print("ERROR : Invalid request. There can be 2 request in order - 1 - setup_connection , 2 - start_election")
            sys.exit(1)
        # Send setup_connection message to cluster socket
        data = kv_message_instance.SerializeToString()
        size = encode_varint(len(data))
        clusterSocket.sendall(size + data)

        print("MSG : InitReplica Message sent to replica server --> ", clusterVar.clusterName)
        clusterSocket.close()
