import zmq

context = zmq.Context()

#  Socket to talk to server
print("Connecting to hello world server…")
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

#  Do 10 requests, waiting each time for a response
a = input("Primer numero: ")
socket.send(a.encode())
socket.recv()
b = input("segundo numero: ")
socket.send(b.encode())
socket.recv()
r = input("Operador: ")
socket.send(r.encode())
r = socket.recv().decode()
print (r)