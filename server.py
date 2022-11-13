import sys
from concurrent import futures
from random import randint
from threading import Timer, Thread, Lock

import grpc

import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc

leader_id: int  # id of leader server
server_id: int  # id of this server
server_addr: str  # server ip:port
term: int  # term of the server
last_vote_term: int
state: int  # 0 - Follower, 1 - Candidate, 2 - Leader
timer_time: int  # ?
timer: Timer
timer = None
hb_timer: Timer
hb_timer = None
suspend_timer: Timer
suspend_timer = None
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
        return pb2.Empty(**{})


class RaftSH(pb2_grpc.RaftServiceServicer):
    def RequestVote(self, request, context):
        global term, last_vote_term, state, is_suspend, leader_id

        if is_suspend:
            return

        term_ = request.term
        id_ = request.id

        # UPDATE TIMER
        if state == 0:
            restart_timer()

        if term <= term_ and last_vote_term < term_:
            if state > 0:
                restart_timer()

            if state == 2:
                close_hb_timer()

            state = 0
            term = term_
            last_vote_term = term_
            leader_id = id_
            print_vote(id_)
            print_state()
            reply = {"term": term, "result": True}
            return pb2.TermResultMessage(**reply)

        reply = {"term": term, "result": False}
        return pb2.TermResultMessage(**reply)

    def AppendEntries(self, request, context):
        global term, last_vote_term, state, leader_id, is_suspend

        if is_suspend:
            return

        term_ = request.term
        id_ = request.id

        # UPDATE TIMER
        if state == 0:
            restart_timer()

        if term_ >= term:
            if state > 0:
                restart_timer()

            if state == 2:
                close_hb_timer()

            state = 0
            term = term_

            if leader_id != id_:
                print_state()

            leader_id = id_

            reply = {"term": term, "result": True}
            return pb2.TermResultMessage(**reply)

        reply = {"term": term, "result": False}
        return pb2.TermResultMessage(**reply)


def append_entry(host):
    global term, server_id, votes, state
    try:
        channel = grpc.insecure_channel(host)
        stub = pb2_grpc.RaftServiceStub(channel)
        response = stub.AppendEntries(pb2.TermIdMessage(term=term, id=server_id))
        if response.term > term:
            close_hb_timer()
            term = response.term
            state = 0
            print_state()
            restart_timer()
    except Exception as e:
        pass


def append_entries():
    global servers, is_suspend

    restart_hb_timer()

    if is_suspend:
        return

    threads = []
    for host in servers.values():
        threads.append(Thread(target=append_entry, args=(host,)))
    [t.start() for t in threads]
    [t.join() for t in threads]


def request_vote(host):
    global term, server_id, votes
    try:
        channel = grpc.insecure_channel(host)
        stub = pb2_grpc.RaftServiceStub(channel)
        response = stub.RequestVote(pb2.TermIdMessage(term=term, id=server_id))
        if response.result:
            votes += 1
    except Exception as e:
        pass


def request_votes():
    global servers, is_suspend
    threads = []
    for host in servers.values():
        threads.append(Thread(target=request_vote, args=(host,)))
    [t.start() for t in threads]
    [t.join() for t in threads]


def start_election():
    global term, state, votes, total_servers, last_vote_term, leader_id, server_id

    if is_suspend:
        state = 0
        return

    print("The leader is dead")

    leader_id = -1
    term += 1
    last_vote_term = term
    state = 1
    votes = 1

    print_state()
    reset_timer()
    print_vote(server_id)
    request_votes()
    if votes > total_servers // 2:
        close_timer()
        state = 2
        leader_id = server_id
        print("Votes received")
        print_state()
        restart_hb_timer()


def close_timer():
    global timer
    if timer is not None:
        timer.cancel()


def restart_timer():
    global timer, timer_time
    close_timer()
    timer = Timer(timer_time / 1000, start_election)
    timer.start()


def reset_timer():
    global timer_time
    timer_time = randint(150, 300)
    restart_timer()


def close_hb_timer():
    global hb_timer
    if hb_timer is not None:
        hb_timer.cancel()


def restart_hb_timer():
    global hb_timer
    close_hb_timer()
    hb_timer = Timer(0.05, append_entries)
    hb_timer.start()


def end_suspend():
    global suspend_timer, is_suspend, state

    is_suspend = False
    suspend_timer.cancel()
    if state == 0:
        restart_timer()


def start_suspend(time_):
    global suspend_timer, is_suspend

    print("Sleeping for", time_, "seconds")
    is_suspend = True
    suspend_timer = Timer(time_, end_suspend)
    suspend_timer.start()


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
    global state, term
    if state == 0:
        print("I am a follower. Term:", term)
    elif state == 1:
        print("I am a candidate. Term:", term)
    elif state == 2:
        print("I am a leader. Term:", term)
    else:
        print("SOMETHING WENT WRONG")


if __name__ == '__main__':
    server_id = int(sys.argv[1])
    leader_id = -1

    server_addr = "Undefined"
    read_config()
    term = 0
    last_vote_term = -1
    state = 0
    is_suspend = False

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftServiceServicer_to_server(RaftSH(), server)
    pb2_grpc.add_ClientServiceServicer_to_server(ClientSH(), server)

    status = server.add_insecure_port(server_addr)
    print(status)
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
        close_hb_timer()
