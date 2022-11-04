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

server_id: int
term: int
last_vote_term: int
state: int  # 0 - Follower, 1 - Candidate, 2 - Leader
timer_time: int  # ?
servers = {}
config_file = "config.conf"


class RaftSH(pb2_grpc.RaftServiceServicer):
    def RequestVote(self, request, context):
        global term, last_vote_term, state

        # UPDATE TIMER

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


def read_config():
    global servers, server_id

    with open(config_file) as fp:
        lines = fp.readlines()
        for line in lines:
            id_, ip, port = line.split()
            servers[int(id_)] = f"{ip}:{port}"

    return servers.pop(server_id)


if __name__ == '__main__':
    server_id = int(sys.argv[1])
    addr = read_config()


