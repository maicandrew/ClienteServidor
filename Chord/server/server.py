import zmq
import sys
import random, string
from getmac import get_mac_address as get_mac
from hashlib import sha256
from os import listdir, makedirs
from os.path import isfile, exists
import json

def hash_id(self, cad):
    return int(sha256(cad.encode()).hexdigest(), 16)

def gen_ran(self, n):
    c = get_mac()
    for _ in range(n):
        c += random.choice(string.ascii_letters+string.digits)
    return c

class Servidor():

    def __init__(self, port, ):
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.successor = None
        self.predecessor = None
        self.PS = 1024*32
        self.hash_range = [None,None]
        self.ID = hash_id(gen_ran(25))

    def download(self, name):
        if exists(self.port+"/"+name):
            file = open(self.port+"/"+name, "rb")
            data = file.read()
            sha = sha256(data).hexdigest()
            self.socket.send_multipart((b"ok", sha.encode(), data))
        else:
            self.socket.send(b"bad")

    def upload(self):
        self.socket.send(b"ok")
        msg = self.socket.recv_multipart()
        msg[0] = msg[0].decode()
        if msg[0] != "end":
            if not exists(self.port):
                makedirs(self.port)
            path = self.port + "/" + msg[0]
            msg[2] = msg[2].decode()
            msg[3] = msg[3].decode()
            dictt["files"][msg[3]] = msg[2]
            f = open(path, "wb")
            self.socket.send(sha256(msg[1]).hexdigest().encode())
            f.write(msg[1])
            f.close()
        else:
            self.socket.send(b"fin")

    def server_connect(self, address, node_address):
        d = {}
        d["action"] = "connect"
        d["address"] = address
        d["id"] = self.ID
        r = {
            "action" : "next",
            "next" : node_address
        }
        while True:
            server = self.context.socket(zmq.REQ)
            server.connect(r["next"])
            server.send_json(d)
            r = server.recv_json()
            print(r)
            if r["state"] == "ok":
                self.predecessor = r["predecessor"]
                self.successor = r["successor"]
                self.hash_range[0] = r["lowest_hash"]
                self.hash_range[1] = self.ID
                server.close()
                print("Conectado")
                return True
            else:
                server.close()

    def add_server(self, node_address, node_ID):
        d = {}
        if self.hash_range[0] >= self.hash_range[1]: #Caso de nodo final
            if self.hash_range[0] < node_ID or node_ID <= self.hash_range[1]:
                for f in listdir(self.port):
                    i = int(f,16)
                    if i <= node_ID:
                        d["parts"][f] = f
                d["state"] = "ok"
                d["lowest_hash"] = hash_range[0]
                self.hash_range[0] = node_ID
                if self.successor != None and self.predecessor != None:
                    d["successor"] = self.successor
                    d["predecessor"] = "tcp://localhost:"+self.port
                    with self.context.socket(zmq.REQ) as s:
                        s.connect(self.successor)
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
                    self.successor = node_address
                else:
                    d["successor"] = "tcp://localhost:"+self.port
                    d["predecessor"] = "tcp://localhost:"+self.port
                    self.successor = node_address
                    self.predecessor = node_address
            else:
                d["state"] = "next"
                if node_ID > self.hash_range[1]:
                    d["next"] = self.successor
                elif node_ID <= self.hash_range[0]:
                    d["next"] = self.predecessor
        else:
            if self.hash_range[0] < node_ID <= self.hash_range[1]:
                for f in listdir(self.port):
                    i = int(f,16)
                    if i <= node_ID:
                        d["parts"][f] = f
                d["state"] = "ok"
                d["lowest_hash"] = self.hash_range[0]
                self.hash_range[0] = node_ID
                d["successor"] = self.successor
                d["predecessor"] = "tcp://localhost:"+self.port
                with self.context.socket(zmq.REQ) as s:
                    s.connect(self.successor)
                    d1 = {
                        "action" : "predecessor",
                        "address" : node_address
                    }
                    print(d1,1)
                    s.send_json(d1)
                    if s.recv() == b"ok":
                        print("Nodo {id} agregado con ip {add}".format(add=node_address, id = node_ID))
                    s.close()            
                self.successor = node_address
            else:
                d["state"] = "next"
                if node_ID > self.hash_range[1]:
                    d["next"] = self.successor
                elif node_ID <= self.hash_range[0]:
                    d["next"] = self.predecessor
        print(d)
        self.socket.send_json(d)

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