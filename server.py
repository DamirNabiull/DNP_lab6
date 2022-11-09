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
votes: int
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
        elif leader_id == -1:
            address = "Haven't leader"
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

        term_ = request.term
        id_ = request.id

        # UPDATE TIMER
        # if state == 0:
        restart_timer()

        if term <= term_ and last_vote_term < term_:
            state = 0
            term = term_
            last_vote_term = term_
            print_vote(id_)
            print_state()
            # restart_timer()
            reply = {"term": term, "result": True}
            return pb2.TermResultMessage(**reply)

        reply = {"term": term, "result": False}
        return pb2.TermResultMessage(**reply)

    def AppendEntries(self, request, context):
        global term, last_vote_term, state, leader_id

        term_ = request.term
        id_ = request.id

        # UPDATE TIMER
        # if state == 0:
        restart_timer()

        if term_ >= term:
            leader_id = id_
            state = 0
            term = term_
            # restart_timer()
            reply = {"term": term, "result": True}
            return pb2.TermResultMessage(**reply)

        reply = {"term": term, "result": False}
        return pb2.TermResultMessage(**reply)


def request_votes():
    # DOPISAT'
    pass


def start_election():
    global term, state, votes, total_servers, last_vote_term, leader_id, server_id

    print("The leader is dead")

    leader_id = -1
    term += 1
    last_vote_term = term
    state = 1
    votes = 1

    restart_timer()
    print_state()
    request_votes()
    print_vote(server_id)

    if votes > total_servers:
        state = 2
        close_timer()


def close_timer():
    global timer
    timer.cancel()


def new_timer():
    global timer
    timer = Timer(timer_time / 1000, start_election)
    timer.start()


def reset_timer():
    global timer_time
    timer_time = randint(150, 300)
    new_timer()


def restart_timer():
    global timer
    close_timer()
    new_timer()


def start_suspend(time_):
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


def print_vote(vote_id):
    print(f"Voted for node {vote_id}")


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
    leader_id = -1

    server_addr = "Undefined"
    read_config()
    term = 0
    state = 0
    is_suspend = False

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # pb2_grpc.add_RaftServiceServicer_to_server(RaftSH(), server)
    pb2_grpc.add_ClientServiceServicer_to_server(ClientSH(), server)

    server.add_insecure_port(server_addr)
    server.start()

    print("The server starts at", server_addr)

    reset_timer()
    print_state()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print('Shutting down')
    finally:
        close_timer()
