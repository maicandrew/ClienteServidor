import zmq
import sys
from hashlib import sha256

context = zmq.Context()

socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")
PS = 1024*1024*2

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
            return sha

def download(name):
    try:
        com = "#d " + name
        socket.send(com.encode())
        f = open(name, "wb")
        data = socket.recv()
        f.write(data)
        f.close()
        socket.send(b".")
        sha_server = socket.recv().decode()
        sha_client = sha256(name)
        if sha_client == sha_server:
            print("Descarga exitosa")
        else:
            raise Exception
    except:
        print("Error")

def upload(name):
    try:
        sha_file = full_hash(name)
        com = "#u " + sha_file
        socket.send(com.encode())
        f = open(name, "rb")
        i = 0
        socket.recv()
        while True:
            f.seek(PS*i)
            data = f.read(PS)
            sha = sha256(data).hexdigest()
            if data:
                i+=1
                socket.send_multipart((sha.encode(),data, sha_file))
                sha_server = socket.recv().decode()
                if sha == sha_server:
                    print("Parte",i,"subida")
                else:
                     raise Exception
            else:
                socket.send(b"end")
                break
        f.close()
    except:
         print("Error")

def listar():
    socket.send(b"#l")
    cad = socket.recv().decode()
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
