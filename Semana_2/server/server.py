import zmq
from hashlib import sha256
from os import listdir
from os.path import isfile

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")
PS = 1024*1024*2

def download(name):
    f = open(name, "rb")
    data = f.read()
    socket.send(data)
    f.close()
    socket.recv()
    socket.send(hashfile(name).encode())

def upload(name):
    socket.send(b"ok")
    while True:
        msg = socket.recv_multipart()
        if msg[0].decode() != "end":
            f = open(msg[0].decode(), "wb")
            socket.send(sha256(msg[1]).hexdigest().encode())
            f.write(msg[1])
            f.close()
        else:
            socket.send(b"fin")
            break
    #socket.send(hashfile(name).encode())

def listar():
    onlyfiles = [f for f in listdir() if isfile(f) and f != "server.py"]
    cad = ",".join(onlyfiles)
    socket.send(cad.encode())

if __name__ == "__main__":
    while True:
        command = socket.recv().decode()
        l = command.split(" ")
        if l[0] == "#d":
            download(l[1])
        elif l[0] == "#u":
            upload(l[1])
        elif l[0] == "#l":
            listar()
