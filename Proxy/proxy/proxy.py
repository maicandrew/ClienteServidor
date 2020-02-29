import zmq
import json

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:7556")
servers = {}

def new_server(address, size):
    servers[address] = size
    socket.send(b"ok")

def client_upload(hash, name, l):
    assignment = {}
    keys = list(servers.keys())
    sl = len(keys)
    for i in range(len(l)):
        if not keys[i%sl] in assignment:
            assignment[keys[i%sl]] = {}
        assignment[keys[i%sl]][i] = l[i].decode()
    socket.send_json(assignment)
    dict = json.load(open("files.json","r"))
    dict["files"][name] = hash
    dict["assignment"][hash] = assignment.copy()
    with open("files.json", "w+") as p:
        p.write(json.dumps(dict,indent=4))
        p.close()

def list():
    dictt = json.load(open("files.json","r"))
    fs = dictt["files"]
    cad = ",".join(fs)
    socket.send(cad.encode())

if __name__ == "__main__":
    while True:
        msg  = socket.recv_multipart()
        msg[0] = msg[0].decode()
        if msg[0] == "#server":
            try:
                new_server(msg[1].decode(), msg[2].decode())
                print("Servidor agregado, servidores en linea:",servers)
            except:
                socket.send(b"bad")
        elif msg[0] == "#client-upload":
            client_upload(msg[1].decode(), msg[2].decode(), msg[3:])
        elif msg[0] == "#list":
            list()
