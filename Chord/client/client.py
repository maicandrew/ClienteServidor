import zmq
import sys
import json
from os import remove
from os.path import exists
from hashlib import sha256

context = zmq.Context()
sockets = {}
proxySocket = context.socket(zmq.REQ)
proxySocket.connect("tcp://localhost:7556")
PS = 1024*32

def full_hash(name):
    f = open(name, "rb")
    sha = sha256()
    i = 0
    while True:
        f.seek(PS*i)
        data = f.read(PS)
        if data:
            i+=1
            sha.update(data)
        else:
            f.close()
            return sha.hexdigest()

def hash_parts(name):
    h = []                  #Lista de hashes de cada una de  las partes
    sha = sha256()          # Variable para el hash del archihvo completo
    with open(name, "rb") as f:
        while True:
            data = f.read(PS)
            if data:
                h.append(sha256(data).hexdigest())       #Se agrega el sha de una parte a la lista
                sha.update(data)                                #Se actualiza el sha del archivo completo agregando una parte
            else:
                break
    return sha.hexdigest(), h

def download(name):
    # try:
    dictt = json.load(open("files.json", "r"))
    if name in dictt["files"]:
        parts = dict(dictt["parts"][dictt["files"][name]].items())
        d = {}
        for server in parts:
            socket = context.socket(zmq.REQ)
            socket.connect(server)
            for p in parts[server]:
                path = "downloaded/"+parts[server][p]
                socket.send_multipart((b"#d", parts[server][p].encode()))
                msg = socket.recv_multipart()
                if msg[0] == b"ok":
                    sha = sha256(msg[2]).hexdigest()
                    if sha == parts[server][p]:
                        msg[1] = msg[1].decode()
                        f = open(path, "wb")
                        f.write(msg[2])
                        f.close()
                        d[int(p)] = msg[1]
                else:
                    print("Error del servidor")
        s = dict(sorted(d.items()))
        file = open("downloaded/"+name,"ab")
        for p in s:
            part = open("downloaded/"+s[p], "rb")
            data = part.read()
            file.seek(PS*p)
            file.write(data)
            part.close()
            remove("downloaded/"+s[p])
        file.close()

        # com = "#d " + name
        # socket.send(com.encode())
        # f = open(name, "wb")
        # data = socket.recv()
        # f.write(data)
        # f.close()
        # socket.send(b".")
        # sha_server = socket.recv().decode()
        # sha_client = sha256(name)
        # if sha_client == sha_server:
        #     print("Descarga exitosa")
        # else:
        #     raise Exception
    # except:
    #     print("Error")

def upload(name):
    try:
        sha_file, parts = hash_parts(name)
        msg = [b"#client-upload", sha_file.encode(), name.encode()]
        for p in parts:
            msg.append(p.encode())
        proxySocket.send_multipart(tuple(msg))
        d = proxySocket.recv_json()
        i = 0
        for server in d:
            socket = context.socket(zmq.REQ)
            socket.connect(server)
            f = open(name, "rb")
            print(server)
            for p in d[server]:
                print(p)
                socket.send(b"#u")
                socket.recv()
                f.seek(PS*int(p))
                data = f.read(PS)
                sha = sha256(data).hexdigest()
                if sha == d[server][p]:
                    print("Bn")
                i+=1
                socket.send_multipart((sha.encode(),data,sha_file.encode(), name.encode()))
                sha_server = socket.recv().decode()
                if sha == sha_server:
                    print("Parte",i,"subida")
                else:
                     raise Exception
            f.close()
            socket.close()
        dict = json.load(open("files.json","r"))
        if not name in dict["files"]:
            dict["files"][name] = sha_file
        if not sha_file in dict["parts"]:
            dict["parts"][sha_file] = d
        with open("files.json", "w+") as file:
            file.write(json.dumps(dict,indent=4))
            file.close()
    except:
         print("Error")
         socket.send(b"end")

def listar():
    proxySocket.send(b"#list")
    cad = proxySocket.recv().decode()
    files = cad.split(",")
    for f in files:
        print(f)

def main():
    if len(sys.argv) >= 2:
        if sys.argv[1] == "upload":
            print("Subiendo...")
            upload(sys.argv[2])
            print("Terminado")
        elif sys.argv[1] == "download":
            download(sys.argv[2])
        elif sys.argv[1] == "listar":
            listar()
    else:
        print("Uso incorrecto")

if __name__ == "__main__":
    main()