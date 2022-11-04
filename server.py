from concurrent import futures
from time import sleep
import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2
import threading
from threading import Timer
from random import randint
import grpc
import zlib
import sys

leader_id: int
server_id: int
server_addr: str
term: int
last_vote_term: int
state: int  # 0 - Follower, 1 - Candidate, 2 - Leader
timer_time: int  # ?
timer: Timer
hb_timer: Timer
is_suspend: bool
servers = {}
total_servers: int
config_file = "config.conf"


class ClientSH(pb2_grpc.ClientServiceServicer):
    def Connect(self, request, context):
        global term, server_id

        reply = {"term": term, "id": server_id}
        return pb2.TermIdMessage(**reply)

    def GetLeader(self, request, context):
        global leader_id, servers, server_id, server_addr

        print("Command from client: getleader")

        if leader_id == server_id:
            address = server_addr
        else:
            address = servers[leader_id]

        print(leader_id, address)

        reply = {"id": leader_id, "address": address}
        return pb2.IdAddressMessage(**reply)

    def Suspend(self, request, context):
        suspend_time = request.value
        print("Command from client: suspend", suspend_time)
        start_suspend(suspend_time)

        # MOVE THIS PRINT TO TIMER
        print("Sleeping for", suspend_time, "seconds")

        return pb2.Empty(**{})


class RaftSH(pb2_grpc.RaftServiceServicer):
    def RequestVote(self, request, context):
        global term, last_vote_term, state

        # UPDATE TIMER
        update_timer()

        term_ = request.term
        id_ = request.id

        if term <= term_ and last_vote_term < term_:
            state = 0
            term = term_
            last_vote_term = term_
            reply = {"term": term, "result": True}
            return pb2.TermResultMessage(**reply)

        reply = {"term": term, "result": False}
        return pb2.TermResultMessage(**reply)


def start_election():
    # ADD ACTIONS
    pass


def read_config():
    global servers, server_id, total_servers, server_addr

    with open(config_file) as fp:
        lines = fp.readlines()
        for line in lines:
            id_, ip, port = line.split()
            servers[int(id_)] = f"{ip}:{port}"

    total_servers = len(servers)
    server_addr = servers.pop(server_id)


def update_timer():
    global timer
    timer = Timer(timer_time / 1000, start_election)


def start_suspend(time_):
    # ADD ACTIONS
    pass


def print_state():
    if state == 0:
        print("I am a follower. Term:", term)
    elif state == 1:
        print("I am a candidate. Term:", term)
    elif state == 1:
        print("I am a leader. Term:", term)
    else:
        print("SOMETHING WENT WRONG")


if __name__ == '__main__':
    server_id = int(sys.argv[1])

    server_addr = "Undefined"
    read_config()

    timer_time = randint(150, 300)
    term = 0
    state = 0

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # pb2_grpc.add_RaftServiceServicer_to_server(RaftSH(), server)
    pb2_grpc.add_ClientServiceServicer_to_server(ClientSH(), server)

    server.add_insecure_port(server_addr)
    server.start()

    print("The server starts at", server_addr)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print('Shutting down')
