import zmq
import sys
import json
from os import remove, mkdir
from os.path import exists, isfile
from hashlib import sha256

PS = 1024*1024*5

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

def download(name, server_address, context):
    # try:
    data = None
    so = context.socket(zmq.REQ)
    d = {"action" : "download", "name": name}
    while True:
        so.connect(server_address)
        so.send_json(d)
        msg = so.recv_multipart()
        if msg[0] == b"ok":
            data = msg[2]
            if sha256(data).hexdigest() != msg[1].decode():
                print("La integridad de los datos se vio comprometida en el envío")
                return
            break
        elif msg[0] == b"bad":
            print(msg[1].decode())
            return
        elif msg[0] == b"next":
            so.disconnect(server_address)
            server_address = msg[1].decode()
    meta = json.loads(data)
    file_path = "downloads/"+meta["name"]
    f = open(file_path,"wb+")
    for i, p in enumerate(meta["parts"]):
        d = {
            "action" : "download",
            "name" : p
        }
        while True:
            so.connect(server_address)
            so.send_json(d)
            msg = so.recv_multipart()
            if msg[0] == b"ok":
                data = msg[2]
                f.write(data)
                if sha256(data).hexdigest() != msg[1].decode():
                    print("La integridad de los datos se vio comprometida en la transmision")
                    f.close()
                    remove(file_path)
                    return
                break
            elif msg[0] == b"bad":
                print(msg[1].decode(), p)
                return
            elif msg[0] == b"next":
                so.disconnect(server_address)
                server_address = msg[1].decode()
    f.close()
    if full_hash(file_path) == meta["hash"]:
        print("Archivo", meta["name"], "descargado con éxito")
    else:
        print("La integridad se vio comprometida, ¿desea borrar el archivo?")
        r = input("(Y/N): ")
        while not r in ["y","Y", "N", "n"]:
            if r == "y" or r == "Y":
                remove(file_path)
            else:
                print("Archivo corrupto descargado")
    # if name in dictt["files"]:
    #     parts = dict(dictt["parts"][dictt["files"][name]].items())
    #     d = {}
    #     for server in parts:
    #         socket = context.socket(zmq.REQ)
    #         socket.connect(server)
    #         for p in parts[server]:
    #             path = "downloaded/"+parts[server][p]
    #             socket.send_multipart((b"#d", parts[server][p].encode()))
    #             msg = socket.recv_multipart()
    #             if msg[0] == b"ok":
    #                 sha = sha256(msg[2]).hexdigest()
    #                 if sha == parts[server][p]:
    #                     msg[1] = msg[1].decode()
    #                     f = open(path, "wb")
    #                     f.write(msg[2])
    #                     f.close()
    #                     d[int(p)] = msg[1]
    #             else:
    #                 print("Error del servidor")
    #     s = dict(sorted(d.items()))
    #     file = open("downloaded/"+name,"ab")
    #     for p in s:
    #         part = open("downloaded/"+s[p], "rb")
    #         data = part.read()
    #         file.seek(PS*p)
    #         file.write(data)
    #         part.close()
    #         remove("downloaded/"+s[p])
    #     file.close()

def upload(name, server_address, context):
    if not isfile(name):
        print("Archivo no encontrado")
        return
    # try:
    so = context.socket(zmq.REQ)
    sha_file, parts = hash_parts(name)
    meta = {
        "name" : name.split("/")[-1],
        "hash" : sha_file,
        "parts" : []
    }
    f = open(name, "rb")
    for i, p in enumerate(parts):
        d = {
            "action" : "upload",
            "name" : p
        }
        f.seek(PS*i)
        data = f.read(PS)
        while True:
            so.connect(server_address)
            so.send_json(d)
            r = so.recv_json()
            if r["state"] == "ok":
                meta["parts"].append(p)
                so.send_multipart((p.encode(), data))
                so.recv()
                print("Parte",i,"subida")
                break
            elif r["state"] == "next":
                so.disconnect(server_address)
                server_address = r["next"]
            elif r["state"] == "badHash":
                print("Hash no valido")
                return
    print("Archivo subido con éxito")
    f = open("files.json", "w+")
    f.write(json.dumps(meta,indent=4))
    f.close()
    f = open("files.json","rb")
    data = f.read()
    h = sha256(data).hexdigest()
    while True:
            so.connect(server_address)
            so.send_json({"action" : "upload", "name" : h})
            r = so.recv_json()
            if r["state"] == "ok":
                so.send_multipart((h.encode(), data))
                so.recv()
                print("El hash para acceder a su archivo es:",h)
                break
            elif r["state"] == "next":
                so.disconnect(server_address)
                server_address = r["next"]
    so.close()

def main():
    if not exists("downloads"):
        mkdir("downloads")
    context = zmq.Context()
    if len(sys.argv) >= 2:
        if sys.argv[1] == "upload":
            print("Subiendo...")
            upload(sys.argv[2], sys.argv[3], context)
        elif sys.argv[1] == "download":
            download(sys.argv[2], sys.argv[3], context)
    else:
        print("Uso incorrecto: el uo correcto es:")
        print("python client.py [action] [filename] [server-address]")

if __name__ == "__main__":
    main()