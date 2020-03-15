import zmq
import sys
import random, string
from getmac import get_mac_address as get_mac
from hashlib import sha256
from os import listdir, makedirs
from os.path import isfile, exists
import json

def hash_id(cad):
    return int(sha256(cad.encode()).hexdigest(), 16)

def gen_ran(n):
    c = "asd"
    print(c)
    for _ in range(n):
        c += random.choice(string.ascii_letters+string.digits)
    return c

class Servidor():
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.path = ip+"-"+port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.ID = hash_id(gen_ran(25))
        self.successor = None
        self.predecessor = None
        self.PS = 1024*1024*5
        self.hash_range = [None,None]

    def download(self, name):
        if exists(self.path+"/"+name):
            file = open(self.path+"/"+name, "rb")
            data = file.read()
            sha = sha256(data).hexdigest()
            self.socket.send_multipart((b"ok", sha.encode(), data))
        else:
            self.socket.send(b"bad")

    def upload(self, name):
        id_name = int(name,16)
        dictt = {}
        if self.hash_range[0] >= self.hash_range[1]: #Caso de nodo final
            if self.hash_range[0] < id_name or id_name <= self.hash_range[1]:
                self.socket.send_json({"state" : "ok", "address" : "tcp://"+self.ip+":"+self.port})
                msg = self.socket.recv_multipart()
                if sha256(msg[1]).hexdigest() == msg[0].decode():
                    f = open(self.path+"/"+msg[0].decode(), "wb")
                    f.write(msg[1])
                    self.socket.send(b"ok")
            else:
                d = {
                    "state" : "next"
                }
                if id_name > self.hash_range[1]:
                    d["next"] = self.successor
                    print("Enviando a sucesor")
                elif id_name <= self.hash_range[0]:
                    d["next"] = self.predecessor
                    print("Enviando a predecesor")
                self.socket.send_json(d)
        else:
            if self.hash_range[0] < id_name <= self.hash_range[1]:
                self.socket.send_json({"state" : "ok", "address" : "tcp://"+self.ip+":"+self.port})
                msg = self.socket.recv_multipart()
                if sha256(msg[1]).hexdigest() == msg[0].decode():
                    f = open(self.path+"/"+msg[0].decode(), "wb")
                    f.write(msg[1])
                    self.socket.send(b"ok")
            else:
                d = {
                    "state" : "next"
                }
                if id_name > self.hash_range[1]:
                    d["next"] = self.successor
                    print("Enviando a sucesor")
                elif id_name <= self.hash_range[0]:
                    d["next"] = self.predecessor
                    print("Enviando a predecesor")
                self.socket.send_json(d)

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
                break
            else:
                server.close()
        so = self.context.socket(zmq.REQ)
        so.connect(self.successor)
        for p in r["parts"]:
            d = {
                "action" : "download",
                "name" : p
            }
            so.send_json(d)
            msg = so.recv_multipart()
            f = open(msg[1].decode(), "wb")
            f.write(msg[2])
            f.close()

    def add_server(self, node_address, node_ID):
        d = {"parts":{}}
        if self.hash_range[0] >= self.hash_range[1]: #Caso de nodo final
            if self.hash_range[0] < node_ID or node_ID <= self.hash_range[1]:
                for f in listdir(self.ip+"-"+self.port):
                    i = int(f,16)
                    if i <= node_ID:
                        d["parts"][f] = f
                d["state"] = "ok"
                d["lowest_hash"] = self.hash_range[0]
                self.hash_range[0] = node_ID
                if self.successor != None and self.predecessor != None:
                    d["successor"] = "tcp://"+self.ip+":"+self.port
                    d["predecessor"] = self.predecessor
                    with self.context.socket(zmq.REQ) as s:
                        s.connect(self.successor)
                        d1 = {
                            "action" : "successor",
                            "address" : node_address
                        }
                        print(d1,2)
                        s.send_json(d1)
                        a = s.recv().decode()
                        if a == b"ok":
                            print(a)
                            print("""Nodo predecesor actualizado:
                            {id} con ip {add}""".format(add=node_address,
                            id = node_ID))
                        s.close()
                    self.predecessor = node_address
                else:
                    d["successor"] = "tcp://"+self.ip+":"+self.port
                    d["predecessor"] = "tcp://"+self.ip+":"+self.port
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
                for f in listdir(self.ip+"-"+self.port):
                    i = int(f,16)
                    if i <= node_ID:
                        d["parts"][f] = f
                d["state"] = "ok"
                d["lowest_hash"] = self.hash_range[0]
                self.hash_range[0] = node_ID
                d["successor"] = "tcp://"+self.ip+":"+self.port
                d["predecessor"] = self.predecessor
                with self.context.socket(zmq.REQ) as s:
                    s.connect(self.successor)
                    d1 = {
                        "action" : "successor",
                        "address" : node_address
                    }
                    print(d1,1)
                    s.send_json(d1)
                    if s.recv() == b"ok":
                        print("Nodo {id} agregado con ip {add}".format(add=node_address, id = node_ID))
                    s.close()            
                self.predecessor = node_address
            else:
                d["state"] = "next"
                if node_ID > self.hash_range[1]:
                    d["next"] = self.successor
                elif node_ID <= self.hash_range[0]:
                    d["next"] = self.predecessor
        print(d)
        self.socket.send_json(d)

if __name__ == "__main__":
    ip = sys.argv[1]
    port = sys.argv[2]
    s = Servidor(ip, port)
    if len(sys.argv) == 3:
        s.hash_range[0] = s.ID
        s.hash_range[1] = s.ID
    elif len(sys.argv) == 4:
        address = "tcp://"+ip+":"+port
        node_connect = "tcp://"+sys.argv[3]
        print("Conectando a la red...")
        if s.server_connect(address, node_connect):
            print("Corriendo...")
        else:
            print("Error al conectarse a la red")
    else:
        print("""Uso incorrecto, el uso correcto es:
        Nodo inicial: python server.py [ip_propia] [puerto_propio]
        Agregar nodo: python server.py [ip_propia] [puerto_propio] [ip_conocido:puerto_conocido]""")
        
    s.socket.bind("tcp://*:"+port)
    print(port)
    print(s.hash_range)
    if not exists(s.path):
        makedirs(s.path)
    while True:
        print(s.successor, s.predecessor)
        print("Escuchando...")
        l = s.socket.recv_json()
        print(l)
        if l["action"] == "connect":
            s.add_server(l["address"], l["id"])
        elif l["action"] == "predecessor":
            s.predecessor = l["address"]
            s.socket.send(b"ok", )
        elif l["action"] == "successor":
            s.successor = l["address"]
            s.socket.send(b"ok")
        elif l["action"] == "upload":
            s.upload(l["name"])
        # elif l[0] == b"#l":
        #     pass
            #listar()