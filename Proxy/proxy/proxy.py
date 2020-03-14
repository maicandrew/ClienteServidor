import zmq
import json

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:7556")
servers = {}

def new_server(address, size):
    servers[address] = int(size)
    socket.send(b"ok")

def client_download(name):
    files = json.load(open("files.json", "r"))
    if name in files["files"]:
        res = files["parts"][files["files"][name]]
        socket.send_json(res)
    else:
        default = {
            "Error":"File not found"
        }
        socket.send_json(default)

def client_upload(hash, name, l):
    assignment = {}
    keys = [*servers.keys()]
    sl = len(keys)
    i = 0
    for _ in range(len(l)):
        while True:
            if not keys[i%sl] in assignment:
                assignment[keys[i%sl]] = {}
            if servers[keys[i%sl]] >= 1:
                assignment[keys[i%sl]][i] = l[i].decode()
                servers[keys[i%sl]] -= 1
                i+= 1
                break
            else:
                i+=1

    socket.send_json(assignment)
    dictt = json.load(open("files.json","r"))
    dictt["files"][name] = hash
    dictt["assignment"][hash] = assignment.copy()
    with open("files.json", "w+") as p:
        p.write(json.dumps(dictt,indent=4))
        p.close()

def list():
    dictt = json.load(open("files.json","r"))
    fs = dictt["files"]
    cad = ",".join(fs)
    socket.send(cad.encode())

if __name__ == "__main__":
    print("Proxy corriendo...")
    while True:
        msg  = socket.recv_multipart()
        msg[0] = msg[0].decode()
        if msg[0] == "#server":
            try:
                new_server(msg[1].decode(), msg[2].decode())
                print("Servidor agregado, servidores en linea:",servers)
            except:
                socket.send(b"bad")
        elif msg[0] == "#client-download":
            print("Action: Client download")
            client_download(msg[1].decode)
        elif msg[0] == "#client-upload":
            print("Action: Client upload")
            client_upload(msg[1].decode(), msg[2].decode(), msg[3:])
        elif msg[0] == "#list":
            print("Action: Listar")
            list()
