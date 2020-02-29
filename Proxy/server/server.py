import zmq
import sys
from hashlib import sha256
from os import listdir, makedirs
from os.path import isfile, exists
import json

port = sys.argv[1]
context = zmq.Context()
socket = context.socket(zmq.REP)
dict = json.load(open("files.json","r"))
PS = 1024*32

def download(name):
    if exists(port+"/"+name):
        file = open(port+"/"+name, "rb")
        data = file.read()
        sha = sha256(data).hexdigest()
        socket.send_multipart((b"ok", sha.encode(), data))
    else:
        socket.send(b"bad")
    # while ans[0] == b"ok":
    #     file = open(name,"rb")
    #     data = file.read()
    #     socket.send_multipart((b"ok", data, hash.encode()))
    #     ans = socket.recv()
    #     if ans != b"ok":
    #         break
    # if ans != b"ok":
    #     socket.send_multipart((b"bad", dict["files"][name].encode()))
    # else:
    #     socket.send_multipart((b"done", dict["files"][name].encode()))
    #f = open(name, "rb")
    #data = f.read()
    #socket.send(data)
    #f.close()
    #socket.recv()
    #socket.send(sha256(name).hexdigest().encode())

def upload():
    socket.send(b"ok")
    msg = socket.recv_multipart()
    msg[0] = msg[0].decode()
    if msg[0] != "end":
        if not exists(port):
            makedirs(port)
        path = port + "/" + msg[0]
        msg[2] = msg[2].decode()
        msg[3] = msg[3].decode()
        dict["files"][msg[3]] = msg[2]
        f = open(path, "wb")
        socket.send(sha256(msg[1]).hexdigest().encode())
        f.write(msg[1])
        f.close()
    else:
        socket.send(b"fin")

def proxy_connect(address, parts):
    socket_proxy = context.socket(zmq.REQ)
    socket_proxy.connect("tcp://localhost:7556")
    socket_proxy.send_multipart((b"#server", address.encode(), parts.encode()))
    if socket_proxy.recv() == b"ok":
        print("Conectado")
        return True
    else:
        print("Error al intentar conectarse al proxy")
        return False

if __name__ == "__main__":
    bind_address = "tcp://*:"+port
    address = "tcp://localhost:"+port
    parts = sys.argv[2]
    print("Conectando al proxy...")
    socket.bind(bind_address)
    if proxy_connect(address, parts):
        print("Corriendo...")
        while True:
            l = socket.recv_multipart()
            if l[0] == b"#d":
                download(l[1].decode())
            elif l[0] == b"#u":
                upload()
            elif l[0] == b"#l":
                listar()