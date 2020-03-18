import zmq
import sys
from hashlib import sha256
from os import listdir, makedirs
from os.path import isfile, exists
import json

PS = 1024*1024*5

def download(name, ip, port, socket):
    if exists(ip+"-"+port+"/"+name):
        file = open(ip+"-"+port+"/"+name, "rb")
        data = file.read()
        sha = sha256(data).hexdigest()
        socket.send_multipart((b"ok", sha.encode(), data))
    else:
        socket.send(b"bad")

def upload(ip, port, socket):
    socket.send(b"ok")
    msg = socket.recv_multipart()
    msg[0] = msg[0].decode()
    if msg[0] != "end":
        if not exists(ip+"-"+port):
            makedirs(ip+"-"+port)
        path = ip+"-"+port + "/" + msg[0]
        msg[2] = msg[2].decode()
        msg[3] = msg[3].decode()
        dictt["files"][msg[3]] = msg[2]
        f = open(path, "wb")
        socket.send(sha256(msg[1]).hexdigest().encode())
        f.write(msg[1])
        f.close()
    else:
        socket.send(b"fin")

def proxy_connect(address, parts):
    socket_proxy = context.socket(zmq.REQ)
    socket_proxy.connect("tcp://"+proxy)
    socket_proxy.send_multipart((b"#server", address.encode(), parts.encode()))
    if socket_proxy.recv() == b"ok":
        print("Conectado")
        return True
    else:
        print("Error al intentar conectarse al proxy")
        return False

def listar():
    onlyfiles = [f for f in listdir() if isfile(f) and f != "server.py"]
    cad = ",".join(onlyfiles)
    socket.send(cad.encode())

if __name__ == "__main__":
    ip  = sys.argv[1]
    port = sys.argv[2]
    proxy = sys.argv[4]
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    dictt = json.load(open("files.json", "r"))
    bind_address = "tcp://*:"+port
    address = "tcp://"+ip+":"+port
    parts = sys.argv[3]
    print("Conectando al proxy...")
    socket.bind(bind_address)
    if proxy_connect(address, parts):
        print("Corriendo...")
        while True:
            l = socket.recv_multipart()
            if l[0] == b"#d":
                download(l[1].decode(), ip, port, socket)
            elif l[0] == b"#u":
                upload(ip, port, socket)
            elif l[0] == b"#l":
                listar()
