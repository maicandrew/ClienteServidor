import time
import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

while True:
    #  Wait for next request from client
    a = socket.recv().decode()
    #  Send reply back to client
    socket.send(b"Ok 1")

    b = socket.recv().decode()

    socket.send(b"Ok 2")

    op = socket.recv().decode()

    r = str(eval(a+op+b))
    socket.send(r.encode())
