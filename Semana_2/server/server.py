import zmq
import hashlib
from os import listdir
from os.path import isfile

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

def hashfile(name):
    f = open(name, "rb")
    sha_signature = hashlib.sha256(f.read()).hexdigest()
    f.close()
    return sha_signature


def download(name):
    f = open(name, "rb")
    data = f.read()
    socket.send(data)
    f.close()
    socket.recv()
    socket.send(hashfile(name).encode())

def upload(name):
    f = open(name, "wb")
    socket.send(b"ok")
    data = socket.recv()
    f.write(data)
    f.close()
    socket.send(hashfile(name).encode())

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