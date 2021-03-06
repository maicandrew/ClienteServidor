import sys
import time
import zmq
from copy import deepcopy
import json
from math import sqrt

def norm(d):
    #Sacar la norma de la lista d
    n = [None for _ in range(len(d))]
    c = 0
    for i in d:
        k = 0
        for j in i:
            k += i[j]**2
        n[c] = sqrt(k)
        c += 1
    return n

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
            self.clus = msg["clusters"]
            assign = [{} for i in range(self.clus)]
            inertia = 0
            for task in range(msg["Totaltasks"]):
                recvm = self.receiver.recv_json()
                work = recvm["assign"]
                print(f"\rTarea recibida {task+1}", end="")
                for c in range(len(work)):
                    inertia += work[c].pop("inertia")
                    for p in work[c]:
                        a = assign[c].get(p)
                        if a:
                            assign[c][p][0] += work[c][p][0]
                            assign[c][p][1] += work[c][p][1]
                        else:
                            assign[c][p] = [work[c][p][0], work[c][p][1]]
            print("")
            clusters = self.average_pos(assign)
            norms = norm(clusters)
            file_new = "new_clusters.txt"
            with open(file_new,"w") as cl_f:
                json.dump({"clusters":clusters,"norms":norms},cl_f)
            self.sender.send_json({"clusters": file_new, "inertia":inertia})

    def average_pos(self, assign):
        clusters = [{} for i in range(self.clus)]
        for c in range(len(assign)):
            for p in assign[c]:
                clusters[c][p] = assign[c][p][0]/assign[c][p][1]
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
