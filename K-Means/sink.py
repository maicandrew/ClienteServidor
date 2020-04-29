import sys
import time
import zmq


class Sink:
    def __init__(self, port = "7556", fan = "localhost:7557"):
        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.bind("tcp://*:"+port)
        self.sender.connect("tcp://"+fan)
        self.cols = 0
        self.clus = 0

    def start(self):
        print("Sink ready...")
        while True:
            msg = self.receiver.recv_json()
            print(msg)
            if "done" in msg:
                print(msg["done"])
                return
            if "tasks" in msg:
                self.clus = msg["clusters"]
                assign = [{} for _ in range(self.clus)]
            for task in range(msg["tasks"]):
                work = self.receiver.recv_json()
                print(f"\rTarea recibida {task+1}", end="")
                for i, c in enumerate(work["assign"]):
                    for p in c:
                        a = assign[i].get(p)
                        if a:
                            assign[i][p][0] += c[p][0]
                            assign[i][p][1] += c[p][1]
                        else:
                            assign[i][p] = [c[p][0], c[p][1]]
            print("")
            clusters = self.average(assign)
            self.sender.send_json({"clusters": clusters})
                #Calcular las distancias de cad msg["points"] a todos los msg["clusters"] en una Matriz
                #for i, point in enumerate(matriz):
                #   min(enumerate(point),key=ittemgetter(1))[0] #Saco la posicion del cluster mas cercano
                #   assign[i] = min  ##En la lista de asignación en la posición del punto, le pongo el numero del cluster
                #   #Quizá todo eso es par el worker
                #

    def average(self, assign):
        clusters = [{} for _ in range(self.clus)]
        for i, c in enumerate(assign):
            for p in c:
                clusters[i][p] = c[p][0]/c[p][1]
        return clusters

if __name__ == "__main__":
    sink = Sink()
    sink.start()

#
# context = zmq.Context()
#
# fan = context.socket(zmq.PULL)
# fan.bind("tcp://*:5558")
#
# # Wait for start of batch
# s = fan.recv()
# print(s)
#
# # Start our clock now
# tstart = time.time()
#
# # Process 100 confirmations
# r = []
# cad = s.decode().split(",")
# d = []
# for i in cad:
#     d.append(int(i))
# result = []
# for i in range(d[0]):
#     result.append([])
#     for j in range(d[1]):
#         result[i].append(0)
#
# print(result)
#
# for i in range(d[0]):
#     s = fan.recv_json()
#     print("Recibido "+str(i)":",s)
#     t = s["turn"]
#     r = s["r"]
#     result[int(t)] = r
#
# print(result)
#
# # Calculate and report duration of batch
# tend = time.time()
# print("Total elapsed time: %d msec" % ((tend-tstart)*1000))
