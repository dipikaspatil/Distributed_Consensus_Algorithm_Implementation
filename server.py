#!usr/bin/python3

import sys
import random
import socket
import threading
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

# Global variables

# Global Dictionary to hold key value pair
globalKeyValueDictionary = {}
# Global mutex to prevent concurrent access of global_key_value_dictionary
globalKeyValueDictMutex = threading.Lock()

# Static Variable to hold all active clusters in the system
globalClusterInfoDict = {}
# Mutex to prevent globalClusterInfoDict
globalClusterInfoDictMutex = threading.Lock()

# Static variable to hold current term of a cluster
globalTerm = 0
# Mutex to prevent globalTerm
globalTermMutex = threading.Lock()

# Static variable to hold global cluster state as - follower, candidate and leader
globalClusterState = "Follower"
# Mutext to prevent globalClusterState
globalClusterStateMutex = threading.Lock()

# Static variable to hold global election timer - random value between 150 to 200 ms
globalElectionTimer = 0
# Mutex to prevent globalElectionTimer
globalElectionTimerMutex = threading.Lock()


# Class to represent Other Cluster Information
class ClusterInfo:
    """Class to represent Replica Information"""

    def __init__(self, cNameIn, cIpIn, cPortIn):
        self.clusterName = cNameIn
        self.clusterIpAddress = cIpIn
        self.clusterPortNumber = int(cPortIn)


# Class to represent Key-Value Cluster Store
class keyValueClusterStore(threading.Thread):
    ''' Class to represent Key-Value Cluster Store '''

    # Constructor
    def __init__(self, incomingSocketIn, incomingSocketIpAddressIn, cNameIn, cIpIn, cPortIn):
        threading.Thread.__init__(self)
        self.incomingSocket = incomingSocketIn
        self.incomingSocketIpAddress = incomingSocketIpAddressIn
        self.clusterName = cNameIn
        self.clusterIp = cIpIn
        self.clusterPort = cPortIn
        self.kv_message_instance = KeyValueClusterStore_pb2.KeyValueMessage()

    # Method (run) works as entry point for each thread - overridden from threading.Thread
    def run(self):
        # Identify message type here by parsing incoming socket message
        data = self.incomingSocket.recv(1)
        size = decode_varint(data)
        self.kv_message_instance.ParseFromString(self.incomingSocket.recv(size))

        # Received Request to First Time Setup Connection
        print(self.kv_message_instance.WhichOneof('key_value_message'))
        if self.kv_message_instance.WhichOneof('key_value_message') == 'setup_connection':
            print("\nSETUP_CONNECTION message received from Admin.", file=sys.stderr)
            # Initialize global_replica_info_dict
            i = 0
            for cluster in self.kv_message_instance.setup_connection.all_clusters:
                if cluster.name == self.clusterName:
                    continue
                globalClusterInfoDict[i] = ClusterInfo(cluster.name, cluster.ip, cluster.port)
                i += 1
            self.incomingSocket.close()

            if DEBUG_STDERR:
                print("Elements of Global Replica Info Dictionary --> ", file=sys.stderr)
                for cKey, cVal in globalClusterInfoDict.items():
                    print(cVal.clusterName, cVal.clusterIpAddress, cVal.clusterPortNumber, file=sys.stderr)



# Class to represent Cluster Node Server
class ClusterNodeServer:
    ''' Class to represent Cluster Node Server '''

    # Constructor
    def __init__(self, argClusterServerNameIn, argClusterServerPortIn):
        self.cName = argClusterServerNameIn
        self.cIp = socket.gethostbyname(socket.gethostname())
        self.cBacklogCount = 100
        self.cPort = argClusterServerPortIn
        self.cthreadList = []  # holds total active thread
        # Create server socket (IPv4 and TCP)
        try:
            self.cSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except:
            print("ERROR : Socket creation failed.")
            sys.exit(1)

    # Bind cluster node socket with given port
    def startCluster(self):
        try:
            self.cSocket.bind(('', self.cPort))
        except:
            print("ERROR : Socket bind failed.")
            sys.exit(1)

        print("\n----------------------------------------------------------------------------------------------------", file=sys.stderr)
        print("Key Value Cluster Node Server Information : \n----------------------------", file=sys.stderr)
        print("Cluster Node Server Name :::::::::::::::::::::::::: \t", self.cName, file=sys.stderr)
        print("Cluster Node Server IP Address :::::::::::::::::::: \t", self.cIp, file=sys.stderr)
        print("Cluster Node Server Port Number ::::::::::::::::::: \t", self.cPort, file=sys.stderr)

        globalTermMutex.acquire()
        globalTerm = 0
        print("Cluster Node Server Current Term :::::::::::::::::: \t", globalTerm, file=sys.stderr)
        globalTermMutex.release()

        globalClusterStateMutex.acquire()
        globalClusterState = "Follower"
        print("Cluster Node Server Current State ::::::::::::::::: \t", globalClusterState, file=sys.stderr)
        globalClusterStateMutex.release()

        globalElectionTimerMutex.acquire()
        globalElectionTimer = random.randrange(150, 200)
        print("Cluster Node Server Current Election Timeout Span : \t", globalElectionTimer, file=sys.stderr)
        globalElectionTimerMutex.release()
        print("----------------------------------------------------------------------------------------------------", file=sys.stderr)

        # Replica socket in listening mode with provided backlog count
        self.cSocket.listen(self.cBacklogCount)
        print("\nCluster Node Server is running.....\n", file=sys.stderr)

    # Accept client request at cluster node socket
    def acceptRequet(self):
        while True:
            try:
                # Accept create new socket for
                # each client request and return tuple
                clientSocket, clientIPAddress = self.cSocket.accept()

                # Handle client's request using separate thread
                keyValueClusterThread = keyValueClusterStore(clientSocket, clientIPAddress, self.cName, self.cIp, self.cPort)

                keyValueClusterThread.start()

                # Save running threads in list
                self.cthreadList.append(keyValueClusterThread)

            except:
                print("ERROR : Socket accept failed.")
                sys.exit(1)

        # Main thread should wait till all threads complete their operation
        for curThread in self.cthreadList:
            curThread.join()

# Starting point for Server - a cluster node
if __name__ == "__main__":
    # Validating command line argument
    if len(sys.argv) != 3: # Server name and port
        print("ERROR : Invalid # of input argument, expected Server Name and Server Port")
        sys.exit(1)

    # Local variables
    argClusterServerName = sys.argv[1]
    argClusterServerPort = int(sys.argv[2])

    # Create cluster node socket and start accepting request using multi-threading
    clusterNodeSocketServer = ClusterNodeServer(argClusterServerName, argClusterServerPort)
    clusterNodeSocketServer.startCluster()
    clusterNodeSocketServer.acceptRequet()

