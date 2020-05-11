import sys
import time
import zmq
from sklearn.metrics.pairwise import cosine_similarity
from numpy import array, zeros
from math import sqrt

def norm(d):
    #Sacar la norma de la lista d
    n = [None for _ in range(len(d))]
    c = 0
    for i in d:
        k = 0
        if "points" in i:
            i.pop("points")
        for j in i:
            k += i[j]**2
        n[c] = sqrt(k)
        c+=1
    return n

class Worker:
    def __init__(self, fan = "localhost:7555", sink = "localhost:7556"):
        self.context = zmq.Context()
        self.fan = self.context.socket(zmq.PULL)
        self.sink = self.context.socket(zmq.PUSH)
        self.fan.connect("tcp://"+fan)
        self.sink.connect("tcp://"+sink)

    def start(self):
        print("Worker running")
        while True:
            msg = self.fan.recv_json()
            points = self.read_data(msg["ifile"])
            assignment = self.assign(points, msg["clusters"])
            self.sink.send_json({"assign": assignment,"t":msg["ifile"]})

    def assign(self, points, clusters):
        assignment = [{"points": []} for _ in range(len(clusters))]
        norm_p = norm(points)
        norm_c = norm(clusters)
        for p, point in enumerate(points):
            index = 0
            min_dis = 0.0-2.0
            for j, cluster in enumerate(clusters):
                dis = self.css(point,cluster, norm_p[p], norm_c[j])
                if dis > min_dis:
                    index = j
                    min_dis = dis
            for k in point:
                a = assignment[index].get(k)
                if a:
                    assignment[index][k][0] += point[k]
                    assignment[index][k][1] += 1
                else:
                    assignment[index][k] = [point[k], 1]
            assignment[index]["points"].append(p)
        print([f"Cluster {i} = {len(assignment[i]['points'])}" for i in range(len(assignment))])
        return assignment

    def css(self, p1, p2, n1, n2):
        com = None
        if len(p1) <= len(p2):
            com = p1
        else:
            com = p2
        dot = 0
        for key in com.keys():
            dot += int(p1.get(key,0)) * int(p2.get(key,0))
        cos_sim = dot / (n1*n2)
        return cos_sim

    def read_data(self, fn):
        points = []
        with open(f"data_part_{fn}.txt","r") as file:
            for line in file:
                line = line[:-1]
                d = {}
                sp = line.split(",")
                for i in sp:
                    i = i[1:-1].split(" ")
                    d[i[0]] = int(i[1])
                points.append(d)
        return points

if __name__ == "__main__":
    worker = Worker()
    worker.start()


#
# context = zmq.Context()
#
# work = context.socket(zmq.PULL)
# work.connect("tcp://localhost:5557")
#
# # Socket to send messages to
# sink = context.socket(zmq.PUSH)
# sink.connect("tcp://localhost:5558")
#
# def start(x):
#     result = []
#     for _ in range(x):
#         result.append(0)
#     return result
#
# def mat_mul(a,b):
#     result = start(len(b[0]))
#     for i in range(len(b[0])):
#         for j in range(len(a)):
#             result[i] += a[j] * b[j][i]
#     return result
#
# # Process tasks forever
# while True:
#     s = work.recv_json()
#     # Simple progress indicator for the viewer
#     row = s["row"]
#     b = s["b"]
#     c = mat_mul(row,b)
#     # Do the work
#     print("Mensaje",s)
#     print("Resultado",c)
#
#     # Send results to sink
#     sink.send_json({"turn":s["turn"], "r":c})
