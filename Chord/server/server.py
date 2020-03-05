import zmq
import sys
import random, string
from getmac import get_mac_address as get_mac
from hashlib import sha256
from os import listdir, makedirs
from os.path import isfile, exists
import json

port = sys.argv[1]
context = zmq.Context()
socket = context.socket(zmq.REP)
successor = None
predecessor = None
PS = 1024*32
hash_range = [None,None]

def hash_id(cad):
    return int(sha256(cad.encode()).hexdigest(), 16)

def gen_ran(n):
    c = get_mac()
    for _ in range(n):
        c += random.choice(string.ascii_letters+string.digits)
    return c

ID = hash_id(gen_ran(25))

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
        dictt["files"][msg[3]] = msg[2]
        f = open(path, "wb")
        socket.send(sha256(msg[1]).hexdigest().encode())
        f.write(msg[1])
        f.close()
    else:
        socket.send(b"fin")

def server_connect(address, node_address):
    global predecessor
    global successor
    d = {}
    d["action"] = "connect"
    d["address"] = address
    d["id"] = ID
    r = {
        "action" : "next",
        "next" : node_address
    }
    while True:
        server = context.socket(zmq.REQ)
        server.connect(r["next"])
        server.send_json(d)
        r = server.recv_json()
        print(r)
        if r["state"] == "ok":
            predecessor = r["predecessor"]
            successor = r["successor"]
            hash_range[0] = r["lowest_hash"]
            hash_range[1] = ID
            server.close()
            print("Conectado")
            return True
        else:
            server.close()

def add_server(node_address, node_ID):
    global ID
    global successor
    global predecessor
    d = {}
    if hash_range[0] >= hash_range[1]: #Caso de nodo final
        if hash_range[0] < node_ID or node_ID <= hash_range[1]:
            for f in listdir(port):
                i = int(f,16)
                if i <= node_ID:
                    d["parts"][f] = f
            d["state"] = "ok"
            d["lowest_hash"] = hash_range[0]
            hash_range[0] = node_ID
            if successor != None and predecessor != None:
                d["successor"] = successor
                d["predecessor"] = "tcp://localhost:"+port
                with context.socket(zmq.REQ) as s:
                    s.connect(successor)
                    d1 = {
                        "action" : "predecessor",
                        "address" : node_address
                    }
                    print(d1,2)
                    s.send_json(d1)
                    a = s.recv().decode()
                    if a == "ok":
                        print(a)
                        print("""Nodo predecesor actualizado:
                        {id} con ip {add}""".format(add=node_address,
                        id = node_ID))
                    s.close()
                successor = node_address
            else:
                d["successor"] = "tcp://localhost:"+port
                d["predecessor"] = "tcp://localhost:"+port
                successor = node_address
                predecessor = node_address
        else:
            d["state"] = "next"
            if node_ID > hash_range[1]:
                d["next"] = successor
            elif node_ID <= hash_range[0]:
                d["next"] = predecessor
    else:
        if hash_range[0] < node_ID <= hash_range[1]:
            for f in listdir(port):
                i = int(f,16)
                if i <= node_ID:
                    d["parts"][f] = f
            d["state"] = "ok"
            d["lowest_hash"] = hash_range[0]
            hash_range[0] = node_ID
            d["successor"] = successor
            d["predecessor"] = "tcp://localhost:"+port
            with context.socket(zmq.REQ) as s:
                s.connect(successor)
                d1 = {
                    "action" : "predecessor",
                    "address" : node_address
                }
                print(d1,1)
                s.send_json(d1)
                if s.recv() == b"ok":
                    print("Nodo {id} agregado con ip {add}".format(add=node_address, id = node_ID))
                s.close()            
            successor = node_address
        else:
            d["state"] = "next"
            if node_ID > hash_range[1]:
                d["next"] = successor
            elif node_ID <= hash_range[0]:
                d["next"] = predecessor
    print(d)
    socket.send_json(d)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        hash_range[0] = ID
        hash_range[1] = ID
    elif len(sys.argv) == 3:
        address = "tcp://localhost:"+port
        node_address = sys.argv[2]
        node_connect = "tcp://"+node_address
        print("Conectando a la red...")
        if server_connect(address, node_connect):
            print("Corriendo...")
        else:
            print("Error al conectarse a la red")
    socket.bind("tcp://*:"+port)
    print(port)
    print(hash_range)
    if not exists(port):
        makedirs(port)
    while True:
        print("Escuchando...")
        l = socket.recv_json()
        print(l)
        if l["action"] == "connect":
            add_server(l["address"], l["id"])
        elif l["action"] == "predecessor":
            predecessor = l["address"]
            socket.send(b"ok")
        elif l["action"] == "successor":
            successor = l["address"]
            socket.send(b"ok")
        elif l[0] == b"#u":
            upload()
        elif l[0] == b"#l":
            pass
            #listar()