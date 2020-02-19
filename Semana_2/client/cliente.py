import zmq
import sys
import hashlib

context = zmq.Context()

socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

def hashfile(name):
    f = open(name, "rb")
    sha_signature = hashlib.sha256(f.read()).hexdigest()
    f.close()
    return sha_signature


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
        sha_client = hashfile(name)
        if sha_client == sha_server:
            print("Descarga exitosa")
        else:
            raise Exception
    except:
        print("Error")

def upload(name):
    try:
        com = "#u " + name
        socket.send(com.encode())
        f = open(name, "rb")
        data = f.read()
        socket.recv()
        socket.send(data)
        f.close()
        sha_server = socket.recv().decode()
        sha_client  = hashfile(name)
        if sha_client == sha_server:
            print("Subida exitosa")
        else:
            raise Exception
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
