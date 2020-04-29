import sys
import time
import zmq
from sklearn.metrics.pairwise import cosine_similarity
from numpy import array, zeros
from math import sqrt

class Worker:
    def __init__(self, file, fan = "localhost:7555", sink = "localhost:7556"):
        self.context = zmq.Context()
        self.fan = self.context.socket(zmq.PULL)
        self.sink = self.context.socket(zmq.PUSH)
        self.fan.connect("tcp://"+fan)
        self.sink.connect("tcp://"+sink)
        self.file_name = file

    def start(self):
        print("Worker running")
        while True:
            msg = self.fan.recv_json()
            points = self.read_data(msg["range"])
            assignment = self.assign(points, msg["clusters"])
            self.sink.send_json({"assign": assignment})

    def assign(self, points, clusters):
        assignment = [{} for _ in range(len(clusters))]
        for point in points:
            index = 0
            min_dis = 0.0-2.0
            for j, cluster in enumerate(clusters):
                dis = self.css(point,cluster)
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
        print([f"Cluster {i} = {len(assignment[i])}" for i in range(len(assignment))])
        return assignment

    def css(self, p1, p2):
        com = {**p1, **p2}
        dot = 0
        sum_p1 = 0
        sum_p2 = 0
        for key in com.keys():
            dot += int(p1.get(key,0)) * int(p2.get(key,0))
            sum_p1 += int(p1.get(key,0))**2
            sum_p2 += int(p2.get(key,0))**2
        cos_sim = dot / (sqrt(sum_p1)*sqrt(sum_p2))
        return cos_sim

    def read_data(self, lines):
        points = []
        with open(self.file_name,"r") as file:
            ln = 0
            for line in file:
                if lines[0] <= ln < lines[1]:
                    line = line[:-1]
                    d = {}
                    sp = line.split(",")
                    for i in sp:
                        i = i[1:-1].split(" ")
                        d[i[0]] = int(i[1])
                    points.append(d)
                ln += 1
        return points

if __name__ == "__main__":
    worker = Worker("test.txt")
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
