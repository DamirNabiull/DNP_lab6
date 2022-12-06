import sys
import grpc
import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc


def get_command_with_args(text: str):
    arr = text.split(' ', 1)
    if len(arr) == 1:
        return arr[0], None
    command = arr[0]
    arguments = arr[1].split(' ')
    return command, arguments


if __name__ == '__main__':
    channel, stub = None, None
    print("The client starts")
    try:
        while True:
            line = input('> ')
            cmd, args = get_command_with_args(line)
            if cmd == 'connect':
                try:
                    channel = grpc.insecure_channel(args[0])
                    stub = pb2_grpc.ClientServiceStub(channel)
                    response = stub.Connect(pb2.Empty())
                    print("Connect: ", response.id)
                except Exception as e:
                    print("Unable to connect")
                    channel, stub = None, None
            elif cmd == 'getleader':
                if not (channel is None or stub is None):
                    response = stub.GetLeader(pb2.Empty())
                    print(response.id, " ", response.address)
                else:
                    print("Not connected")
            elif cmd == 'suspend':
                if not (channel is None or stub is None):
                    response = stub.Suspend(pb2.IntMessage(value=int(args[0])))
                else:
                    print("Not connected")
            elif cmd == 'quit':
                print('The client ends')
                sys.exit(0)
            elif cmd == 'setval':
                if not (channel is None or stub is None):
                    response = stub.SetVal(pb2.SetValMessage(key=args[0], value=args[1]))
                    if not response.success:
                        print(f'{response.success}')
                else:
                    print("Not connected")
            elif cmd == 'getval':
                if not (channel is None or stub is None):
                    response = stub.GetVal(pb2.GetValMessage(key=args[0]))
                    if response.success:
                        print(f'{response.value}')
                    else:
                        print("None")
                else:
                    print("Not connected")
            else:
                print('Unacceptable command')
    except KeyboardInterrupt:
        print('\nThe client ends')
