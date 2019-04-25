#!usr/bin/python3

import sys
import time
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
# Static variable to check if Cluster Node is leader
#globalIsCNodeLeader = False

# Mutex to prevent globalIsLeader
#globalIsCNodeLeaderMutex = threading.Lock()

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

# Static variable to check if globalElectionTimer changed
isGlobalElectionTimerChanged = False
isGlobalElectionTimerChangedMutex = threading.Lock()

# Static variable to keep check on vote count for specific term
globalTermVoteCount = {}
# Mutex to prevent globalTermVoteCount
globalTermVoteCountMutex = threading.Lock()

# Static variable to check if server has voted for specific term or not
globalTermVoted = {}
# Mutex to prevent globalTermVoted
globalTermVotedMutex = threading.Lock()

# SET to store repair cluster node
globalRepairClusterNode = set()
# Mutex to prevent globalRepairClusterNode
globalRepairClusterNodeMutex = threading.Lock()

# Class to monitor client request
class ClientRequest:
    def __init__ (self, requestTransIdIn, requestKeyIn, requestCntIn, requestIpIn, requestPortIn, requestTypeIn):
        self.requestTransId = requestTransIdIn
        self.requestKey = requestKeyIn
        self.requestCnt = requestCntIn
        self.requestIp = requestIpIn
        self.requestPort = requestPortIn
        self.requestType = requestTypeIn

# Dictionary to hold object of client request - key - requestKey
globalClientRequestDict = {}
# Mutex to prevent globalClientRequestDict
globalClientRequestDictMutex = threading.Lock()

# Class to represent Log
class Log:
    """Class to represent Log"""

    def __init__(self, keyIn, valueIn, termIn, indxIn):
        self.key = keyIn
        self.value = valueIn
        self.term = termIn
        self.indx = indxIn

# Static variable to hold logs
globalLogVect = []
globalLogVectMutex = threading.Lock()

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
        self.persistentFileName =  cNameIn + "_file"

    # Method (run) works as entry point for each thread - overridden from threading.Thread
    def run(self):
        global globalElectionTimer, globalTerm, globalClusterState, globalTermVoted, globalTermVoteCount, isGlobalElectionTimerChanged, globalClientRequestDict

        # Identify message type here by parsing incoming socket message
        data = self.incomingSocket.recv(1)
        size = decode_varint(data)
        self.kv_message_instance.ParseFromString(self.incomingSocket.recv(size))

        # Received Request to First Time Setup Connection
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
        elif self.kv_message_instance.WhichOneof('key_value_message') == 'start_election':
            print("\nSTART_ELECTION message received from Admin.", file=sys.stderr)

            # Vote for globalElectionTimer
            while True:
                t_end = time.time() + globalElectionTimer
                if DEBUG_STDERR:
                    print("\nCluster Node ", self.clusterName, " waiting for ", globalElectionTimer, " seconds at --> ", time.asctime(time.localtime(time.time())), file=sys.stderr)
                while time.time() < t_end:
                    time.sleep(1)
                    isGlobalElectionTimerChangedMutex.acquire()
                    if isGlobalElectionTimerChanged:
                        t_end = time.time() + globalElectionTimer
                        isGlobalElectionTimerChanged = False
                    isGlobalElectionTimerChangedMutex.release()

                #time.sleep(globalElectionTimer)
                print("\nFor Cluster Node ", self.clusterName, "globalElectionTimer timeout happened at --> ", time.asctime(time.localtime(time.time())), file=sys.stderr)

                # Local variable
                sendRequestForVote = False

                # Increase the term
                globalTermMutex.acquire()
                #if globalTerm not in globalTermVoted:
                globalTerm = globalTerm + 1
                # Check if server has already voted for this term
                globalTermVotedMutex.acquire()
                # if not - vote yourself
                if globalTerm not in globalTermVoted:
                    #globalTerm = self.kv_message_instance.start_election.term
                    # Vote yourself
                    globalTermVoted[globalTerm] = True

                    # Increase total count for specific term
                    globalTermVoteCountMutex.acquire()
                    globalTermVoteCount[globalTerm] = 1
                    globalTermVoteCountMutex.release()

                    # Change current state to Candidate
                    globalClusterStateMutex.acquire()
                    globalClusterState = "Candidate"
                    if DEBUG_STDERR:
                        print("Server ", self.clusterName, " changed it's current state to ", globalClusterState, file=sys.stderr)
                    sendRequestForVote = True
                    globalClusterStateMutex.release()

                globalTermVotedMutex.release()
                globalTermMutex.release()

                # Request vote to all clusters
                if sendRequestForVote:
                    # Create KeyValueMessage object and wrap setup_connection object inside it
                    KvReqeustVoteMessage = KeyValueClusterStore_pb2.KeyValueMessage()
                    KvReqeustVoteMessage.request_vote.term = globalTerm
                    KvReqeustVoteMessage.request_vote.clusterName = self.clusterName
                    KvReqeustVoteMessage.request_vote.clusterIp = self.clusterIp
                    KvReqeustVoteMessage.request_vote.clusterPort = self.clusterPort

                    globalClusterInfoDictMutex.acquire()
                    # Create message - request vote and send to all servers
                    for clusterKey, clusterVal in globalClusterInfoDict.items():
                        # Create client socket IPv4 and TCP
                        try:
                            requestVoteSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        except:
                            print("ERROR : Socket creation failed.")
                            sys.exit(1)

                        # Connect client socket to server using 3 way handshake
                        try:
                            requestVoteSocket.connect((clusterVal.clusterIpAddress, int(clusterVal.clusterPortNumber)))
                        except:
                            #print("ERROR : Socket creation failed for ", clusterVal.clusterName)
                            continue
                        # Send setup_connection message to cluster socket
                        data = KvReqeustVoteMessage.SerializeToString()
                        size = encode_varint(len(data))
                        requestVoteSocket.sendall(size + data)

                        print("MSG : Request Vote Message sent to replica server --> ", clusterVal.clusterName)
                        requestVoteSocket.close()
                    globalClusterInfoDictMutex.release()

        elif self.kv_message_instance.WhichOneof('key_value_message') == 'request_vote':
            #print("\nREQUEST_VOTE message received from ", self.kv_message_instance.request_vote.clusterName, " for term ", self.kv_message_instance.request_vote.term, file=sys.stderr)
            requestedVoteTerm = self.kv_message_instance.request_vote.term
            requestVoteIp = self.kv_message_instance.request_vote.clusterIp
            requestVotePort = self.kv_message_instance.request_vote.clusterPort

            # Check if server has already voted for this term

            # Create message - response vote and send reply to server
            # Create KeyValueMessage object and wrap setup_connection object inside it
            KvResponseVoteMessage = KeyValueClusterStore_pb2.KeyValueMessage()
            KvResponseVoteMessage.response_vote.term = requestedVoteTerm
            KvResponseVoteMessage.response_vote.clusterName = self.clusterName

            # If server has not yet voted for the requested term and server's term is currently smaller or equal to requested term
            # vote to server - True (yes) else False (no) TODO - need to change later considering log

            globalTermMutex.acquire()
            globalTermVotedMutex.acquire()
            if requestedVoteTerm not in globalTermVoted and globalTerm <= requestedVoteTerm:
                globalTerm = requestedVoteTerm
                globalTermVoted[requestedVoteTerm] = True
                KvResponseVoteMessage.response_vote.voteStatus = "YES"

                # Increase total count for specific term
                globalTermVoteCountMutex.acquire()
                globalTermVoteCount[globalTerm] = 1
                globalTermVoteCountMutex.release()

                # Change current state to Candidate
                globalClusterStateMutex.acquire()
                globalClusterState = "Follower"
                if DEBUG_STDERR:
                    print("Server ", self.clusterName, " changed it's current state to ", globalClusterState, file=sys.stderr)
                globalClusterStateMutex.release()
            else:
                KvResponseVoteMessage.response_vote.voteStatus = "NO"
            globalTermVotedMutex.release()
            globalTermMutex.release()

            # Create socket and send response
            # Create client socket IPv4 and TCP
            try:
                responseVoteSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            except:
                print("ERROR : Socket creation failed.")
                sys.exit(1)

            # Connect client socket to server using 3 way handshake
            responseVoteSocket.connect((requestVoteIp, requestVotePort))
            # Send setup_connection message to cluster socket
            data = KvResponseVoteMessage.SerializeToString()
            size = encode_varint(len(data))
            responseVoteSocket.sendall(size + data)

            print("MSG : Response Vote Message as " , KvResponseVoteMessage.response_vote.voteStatus, " sent to cluster server --> ", self.kv_message_instance.request_vote.clusterName)
            responseVoteSocket.close()
        elif self.kv_message_instance.WhichOneof('key_value_message') == 'response_vote':
            print("\nResponse Vote message received from .", self.kv_message_instance.response_vote.clusterName, " for term ",
                  self.kv_message_instance.response_vote.term, "as ", self.kv_message_instance.response_vote.voteStatus, file=sys.stderr)

            # Local variable
            voteStatus = self.kv_message_instance.response_vote.voteStatus
            voteCount = 0

            if voteStatus == "YES": # True / yes
                # Increase total count for specific term
                globalTermVoteCountMutex.acquire()
                globalTermVoteCount[globalTerm] = globalTermVoteCount[globalTerm] + 1
                voteCount = globalTermVoteCount[globalTerm]
                globalTermVoteCountMutex.release()

                if voteCount == MAJORITY_CLUSTER_COUNT:
                    # Elect oneself as leader and send heartbeat message to all - keep sending it on regular interval - 100ms
                    # Change current state to Candidate
                    globalClusterStateMutex.acquire()
                    globalClusterState = "Leader"
                    #globalIsCNodeLeaderMutex.acquire()
                    #globalIsCNodeLeader = True
                    #globalIsCNodeLeaderMutex.release()
                    if DEBUG_STDERR:
                        print("MSG : ", self.clusterName, " has changed it's state to ", globalClusterState, " for term ", globalTerm, file=sys.stderr)
                    globalClusterStateMutex.release()

                # Send heartbeat_message to all servers
                while True:
                    #globalIsCNodeLeaderMutex.acquire()
                    if globalClusterState == "Leader":
                        print("Leader ", self.clusterName, "Sending Heartbeat Message", file=sys.stderr)
                        globalElectionTimerMutex.acquire()
                        globalElectionTimer = random.randrange(ELECTION_TIMER_MIN, ELECTION_TIMER_MAX)
                        globalElectionTimer = globalElectionTimer / 100
                        isGlobalElectionTimerChangedMutex.acquire()
                        isGlobalElectionTimerChanged = True
                        isGlobalElectionTimerChangedMutex.release()
                        if DEBUG_STDERR:
                            print("Leader Cluster Node Server Current Election Timeout reset to : \t", globalElectionTimer, file=sys.stderr)
                        globalElectionTimerMutex.release()

                        # Create KeyValueMessage object and wrap setup_connection object inside it
                        KvHeartBeatMessage = KeyValueClusterStore_pb2.KeyValueMessage()
                        KvHeartBeatMessage.heartbeat_message.term = globalTerm
                        KvHeartBeatMessage.heartbeat_message.clusterName = self.clusterName

                        # Get the last entry from globalLogVect
                        globalLogVectMutex.acquire()
                        if len(globalLogVect):
                            logObj = globalLogVect[len(globalLogVect)-1]
                            KvHeartBeatMessage.heartbeat_message.entry.key = logObj.key
                            KvHeartBeatMessage.heartbeat_message.entry.values = logObj.value
                            KvHeartBeatMessage.heartbeat_message.entry.term = logObj.term
                            KvHeartBeatMessage.heartbeat_message.entry.indx = logObj.indx
                            KvHeartBeatMessage.heartbeat_message.leaderIp = self.clusterIp
                            KvHeartBeatMessage.heartbeat_message.leaderPort = self.clusterPort
                        else:
                            KvHeartBeatMessage.heartbeat_message.entry.indx = -1
                        globalLogVectMutex.release()

                        globalClusterInfoDictMutex.acquire()
                        # Create message - request vote and send to all servers
                        for clusterKey, clusterVal in globalClusterInfoDict.items():
                            globalRepairClusterNodeMutex.acquire()
                            if clusterVal.clusterName in globalRepairClusterNode:
                                globalRepairClusterNodeMutex.release()
                                continue
                            globalRepairClusterNodeMutex.release()
                            # Create client socket IPv4 and TCP
                            try:
                                heartBeatSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            except:
                                print("ERROR : Socket creation failed.")
                                sys.exit(1)

                            # Connect client socket to server using 3 way handshake
                            try:
                                heartBeatSocket.connect((clusterVal.clusterIpAddress, int(clusterVal.clusterPortNumber)))
                            except:
                                #print("ERROR : Socket creation failed for ", clusterVal.clusterName)
                                continue
                            # Send setup_connection message to cluster socket
                            data = KvHeartBeatMessage.SerializeToString()
                            size = encode_varint(len(data))
                            heartBeatSocket.sendall(size + data)
                            heartBeatSocket.close()
                        globalClusterInfoDictMutex.release()
                    else:
                        break
                    #globalIsCNodeLeaderMutex.release()
                    time.sleep(HEARTBEAT_TIME)
        elif self.kv_message_instance.WhichOneof('key_value_message') == 'heartbeat_message':
            if DEBUG_STDERR:
                print("Heartbeat Message received from leader --> ", self.kv_message_instance.heartbeat_message.clusterName, " for term ",
                      self.kv_message_instance.heartbeat_message.term, file=sys.stderr)

            # if current term of cluster is greater than that of heartbeat message term - do nothing otherwise reset the globalElectionTimer
            hearbeatTerm = self.kv_message_instance.heartbeat_message.term
            considerLog = False
            globalTermMutex.acquire()
            if globalTerm <= hearbeatTerm:
                print("self.kv_message_instance.heartbeat_message.append_entry.indx -->", self.kv_message_instance.heartbeat_message.append_entry.indx, file=sys.stderr)
                if self.kv_message_instance.heartbeat_message.entry.indx != -1:
                    considerLog = True
                globalTerm = hearbeatTerm

                globalClusterStateMutex.acquire()
                globalClusterState = "Follower"
                globalClusterStateMutex.release()

                globalElectionTimerMutex.acquire()
                globalElectionTimer = random.randrange(ELECTION_TIMER_MIN, ELECTION_TIMER_MAX)
                globalElectionTimer = globalElectionTimer / 100
                isGlobalElectionTimerChangedMutex.acquire()
                isGlobalElectionTimerChanged = True
                isGlobalElectionTimerChangedMutex.release()
                if DEBUG_STDERR:
                    print("Cluster Node Server Current Election Timeout reset to : \t", globalElectionTimer, file=sys.stderr)
                globalElectionTimerMutex.release()
            globalTermMutex.release()
            # Check the log
            # Get last entry of globalLogVect
            if considerLog:
                response = True
                #for entry in self.kv_message_instance.append_entry:
                reqKey = self.kv_message_instance.heartbeat_message.entry.key
                reqValue = self.kv_message_instance.heartbeat_message.entry.value
                reqTerm = self.kv_message_instance.heartbeat_message.entry.term
                reqIndx = self.kv_message_instance.heartbeat_message.entry.indx

                globalLogVectMutex.acquire()
                if len(globalLogVect):
                    # Create KeyValueMessage object and wrap setup_connection object inside it
                    if len(globalLogVect) > reqIndx :
                        if globalLogVect[reqIndx].indx != reqIndx:
                            response = False
                    elif globalLogVect[len(globalLogVect)-1].indx != (reqIndx -1):
                        response = False
                    else:
                        response = True
                        globalLogVect.append(Log(reqKey, reqValue, reqTerm, reqIndx))
                else:
                    response = True
                    globalLogVect.append(Log(reqKey, reqValue, reqTerm, reqIndx))
                globalLogVectMutex.release()

                # Send HeartBeat Response Message
                #if not response:
                globalRepairClusterNodeMutex.acquire()
                globalRepairClusterNode.add(self.clusterName)
                globalRepairClusterNodeMutex.release()
                reqLeaderIp = self.kv_message_instance.heartbeat_message.leaderIp
                reqLeaderPort = self.kv_message_instance.heartbeat_message.leaderPort

                KvHeartBeatResponseMessage = KeyValueClusterStore_pb2.KeyValueMessage()
                KvHeartBeatResponseMessage.append_entry_response.key = reqKey
                KvHeartBeatResponseMessage.append_entry_response.value = reqValue
                KvHeartBeatResponseMessage.append_entry_response.indx = reqIndx
                KvHeartBeatResponseMessage.append_entry_response.message = "FAIL" if not response else "SUCCESS"
                KvHeartBeatResponseMessage.append_entry_response.clusterNodeIp = self.clusterIp
                KvHeartBeatResponseMessage.append_entry_response.clusterNodePort = self.clusterPort
                KvHeartBeatResponseMessage.append_entry_response.clusterNodeName = self.clusterName
                try:
                    heartBeatResponseSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                except:
                    print("ERROR : Socket creation failed.")
                    sys.exit(1)

                # Connect client socket to server using 3 way handshake
                heartBeatResponseSocket.connect((reqLeaderIp, reqLeaderPort))
                # Send setup_connection message to cluster socket
                data = KvHeartBeatResponseMessage.SerializeToString()
                size = encode_varint(len(data))
                KvHeartBeatResponseMessage.sendall(size + data)
                KvHeartBeatResponseMessage.close()

        elif self.kv_message_instance.WhichOneof('key_value_message') == 'client_request':
            requestType = self.kv_message_instance.client_request.request_type

            if requestType == "PUT":
                reqKey = self.kv_message_instance.client_request.put_request.key
                reqValue = self.kv_message_instance.client_request.put_request.value
                if DEBUG_STDERR:
                    print(requestType, " Request Message received from client for key --> ", reqKey, " and value ",
                          reqValue, file=sys.stderr)

                # Append leader's log
                globalLogVectMutex.acquire()
                logObj = Log(reqKey, reqValue, globalTerm, len(globalLogVect))
                globalClientRequestDictMutex.acquire()
                globalClientRequestDict[reqKey] = ClientRequest(reqKey,1,self.kv_message_instance.client_request.clientIp, self.kv_message_instance.client_request.clientPort, "PUT")
                globalClientRequestDictMutex.release()
                globalLogVect.append(logObj)
                globalLogVectMutex.release()
            elif requestType == "GET": # TODO
                reqKey = self.kv_message_instance.client_request.get_request.key
                if DEBUG_STDERR:
                    print(requestType, " Request Message received from client for key --> ", reqKey, file=sys.stderr)
        elif self.kv_message_instance.WhichOneof('key_value_message') == 'append_entry_response':
            if DEBUG_STDERR:
                print("AppendEntryResponse Message received from --> ", self.kv_message_instance.append_entry_response.clusterNodeName, " for indx ",
                      self.kv_message_instance.append_entry_response.indx, " with result as ", self.kv_message_instance.append_entry_response.result,
                      file=sys.stderr)
            # Check if client request is in dictionary i.e. client is waiting for response from leader
            clientReqKey = self.kv_message_instance.append_entry_response.key
            clientReqValue = self.kv_message_instance.append_entry_response.value
            clientReqMessage = self.kv_message_instance.append_entry_response.message

            if clientReqMessage == "SUCCESS":
                globalClientRequestDictMutex.acquire()
                if clientReqKey in globalClientRequestDict:
                    globalClientRequestDict[clientReqKey].requestCnt = globalClientRequestDict[clientReqKey].requestCnt + 1
                    commitEntry = False  # Variable to send response to all other IPs
                    if globalClientRequestDict[clientReqKey].requestCnt >= MAJORITY_CLUSTER_COUNT:
                        commitEntry = True
                        # Commit entry into persistent storage
                        fo = open("./" + self.persistentFileName, "w")
                        commit = False
                        for line in fo.readline():
                            key = str(line).split(':')[0]
                            value = str(line).split(':')[1].strip()
                            if key == clientReqKey:
                                line.replace(value, clientReqValue)
                                commit = True
                                break
                        if not commit:
                            fo.write(clientReqKey + ":" + clientReqValue)
                        fo.close()

                        # Response back to client
                        KvLeaderResponseMessage = KeyValueClusterStore_pb2.KeyValueMessage
                        KvLeaderResponseMessage.leader_response.transaction_id = globalClientRequestDict[clientReqKey].requestTransId
                        KvLeaderResponseMessage.leader_response.key = clientReqKey
                        KvLeaderResponseMessage.leader_response.message = "PUT request for transaction id " + globalClientRequestDict[clientReqKey].requestTransId + "Completed Successfully"
                        KvLeaderResponseMessage.leader_response.request_type = "PUT"

                        try:
                            leaderResponseSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        except:
                            print("ERROR : Socket creation failed.")
                            sys.exit(1)

                        # Connect client socket to server using 3 way handshake
                        leaderResponseSocket.connect((globalClientRequestDict[clientReqKey].requestIp, globalClientRequestDict[clientReqKey].requestPort))
                        # Send setup_connection message to cluster socket
                        data = KvLeaderResponseMessage.SerializeToString()
                        size = encode_varint(len(data))
                        KvLeaderResponseMessage.sendall(size + data)
                        KvLeaderResponseMessage.close()

                        # Erase entry from dictionary
                        del globalClientRequestDict[clientReqKey]
                    globalClientRequestDictMutex.release()

                    if commitEntry:
                        # Create KeyValueMessage object and wrap setup_connection object inside it
                        KvCommitEntryMessage = KeyValueClusterStore_pb2.KeyValueMessage()
                        KvCommitEntryMessage.key = clientReqKey
                        KvCommitEntryMessage.value = clientReqValue
                        # Send commit entry message to all cluster nodes
                        globalClusterInfoDictMutex.acquire()
                        # Create message - request vote and send to all servers
                        for clusterKey, clusterVal in globalClusterInfoDict.items():
                            globalRepairClusterNodeMutex.acquire()
                            if clusterVal.clusterName in globalRepairClusterNode:
                                globalRepairClusterNodeMutex.release()
                                continue
                            globalRepairClusterNodeMutex.release()
                            # Create client socket IPv4 and TCP
                            try:
                                commitEntrySocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            except:
                                print("ERROR : Socket creation failed.")
                                sys.exit(1)

                            # Connect client socket to server using 3 way handshake
                            try:
                                commitEntrySocket.connect((clusterVal.clusterIpAddress, int(clusterVal.clusterPortNumber)))
                            except:
                                # print("ERROR : Socket creation failed for ", clusterVal.clusterName)
                                continue
                            # Send setup_connection message to cluster socket
                            data = KvCommitEntryMessage.SerializeToString()
                            size = encode_varint(len(data))
                            commitEntrySocket.sendall(size + data)
                            commitEntrySocket.close()
                        globalClusterInfoDictMutex.release()
            elif clientReqMessage == "FAIL":
                if DEBUG_STDERR:
                    print("AppendEntryResponse FAIL Message received from --> ", self.kv_message_instance.append_entry_response.clusterNodeName, " for indx ",
                      self.kv_message_instance.append_entry_response.indx, file=sys.stderr)
                #TODO - LOG correction

        elif self.kv_message_instance.WhichOneof('key_value_message') == 'commit_entry':
            if DEBUG_STDERR:
                print("CommitEntry Message received from Leader key --> ", self.kv_message_instance.commit_entry.key, " value ",
                      self.kv_message_instance.commit_entry.value, file=sys.stderr)
            fo = open("./" + self.persistentFileName, "w")
            commit = False
            for line in fo.readline():
                key = str(line).split(':')[0]
                value = str(line).split(':')[1].strip()
                if key == self.kv_message_instance.commit_entry.key:
                    line.replace(value, self.kv_message_instance.commit_entry.value)
                    commit = True
                    break
            if not commit:
                fo.write(self.kv_message_instance.commit_entry.key + ":" + self.kv_message_instance.commit_entry.value)
            fo.close()



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
        global globalElectionTimer, globalTerm, globalClusterState, globalTermVoted, globalTermVoteCount

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
        globalElectionTimer = random.randrange(ELECTION_TIMER_MIN, ELECTION_TIMER_MAX)
        globalElectionTimer = globalElectionTimer/100
        print("Cluster Node Server Current Election Timeout Span : \t", globalElectionTimer, file=sys.stderr)
        globalElectionTimerMutex.release()
        print("----------------------------------------------------------------------------------------------------", file=sys.stderr)

        # Replica socket in listening mode with provided backlog count
        self.cSocket.listen(self.cBacklogCount)
        print("\nCluster Node Server is running.....\n", file=sys.stderr)

    # Accept client request at cluster node socket
    def acceptRequet(self):
        global globalElectionTimer, globalTerm, globalClusterState, globalTermVoted, globalTermVoteCount

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

