import zmq
import difflib as dl
from math import ceil
import time
import sys
import os
from copy import deepcopy
import random
#
# def start(x,y):
#     result = []
#     for i in range(x):
#         result.append([])
#         for _ in range(y):
#             result[i].append(0)
#     return result
#
# def mat_mul(a,b):
#     result = start(len(a),len(b))
#     for i in range(len(a)):
#         # iterate through columns of Y
#         for j in range(len(b[0])):
#             # iterate through rows of Y
#             for k in range(len(b)):
#                 result[i][j] += a[i][k] * b[k][j]
#     return result

def parse_line(line):
    line = line[:-1]
    d = {}
    sp = line.split(",")
    for m in sp:
        m = m[1:-1].split(" ")
        d[m[0]] = int(m[1])
    return d

def count_f(cad):
    c = 0
    for f in os.listdir():
        if cad in f:
            c += 1
    return c

class Fan:
    def __init__(self, data_file, k, sinkPull = "tcp://*:7557", sinkPush = "tcp://localhost:7556", workers = "tcp://*:7555", max_it = 1000, tasks = 100):
        self.dataset = data_file
        self.tol = 10
        self.k = k
        self.tasks = tasks
        self.max_it = max_it
        clusters = self.sample(k, tasks)
        self.clusters = clusters
        self.totalTasks = count_f("data_part_")
        self.context = zmq.Context()
        self.sinkPush = self.context.socket(zmq.PUSH)
        self.sinkPull = self.context.socket(zmq.PULL)
        self.workers = self.context.socket(zmq.PUSH)
        self.sinkPull.bind(sinkPull)
        self.sinkPush.connect(sinkPush)
        self.workers.bind(workers)

    def start(self):
        print("Press enter when workers and sink are ready...", end='')
        input()
        iteration = 0
        done = False
        new_clusters = deepcopy(self.clusters)
        while not done:
            st = time.time()
            print(f"Iteration: {iteration+1}")
            dsink = {
                "tasks" : self.tasks,
                "Totaltasks" : self.totalTasks,
                "clusters" : len(self.clusters)
            }
            self.sinkPush.send_json(dsink)
            for i in range(self.totalTasks):
                dworker = {
                    "clusters" : new_clusters,
                    "ifile" : i
                }
                self.workers.send_json(dworker)
            res = self.sinkPull.recv_json()
            new_clusters = res["clusters"]
            if self.compareAssignment(new_clusters):
                done = True
            elif iteration > self.max_it:
                print("Limite de iteraciones")
                self.sinkPush.send_json({
                "done" : "Tarea terminada"
                })
                break
            else:
                self.clusters = deepcopy(new_clusters)
                iteration += 1
            et = time.time()
            print("Time:",et-st)
        print("Terminado")
        print(self.clusters)

    def compareAssignment(self, new):
        print("Comparando clusters...")
        for i in range(len(new)):
            print(i)
            if not "points" in self.clusters[i]:
                return False
            simil = dl.SequenceMatcher(new[i]["points"],self.clusters[i]["points"]).ratio()
            print(simil)
            if (1-simil)*100 > self.tol:
                return False
        return True

    def read_data(self, file_name):
        dataset = []
        with open(file_name,"r") as file:
            for line in file:
                line = line[:-1]
                d = {}
                sp = line.split(",")
                for i in sp:
                    i = i[1:-1].split(" ")
                    d[int(i[0])] = int(i[1])
                dataset.append(d)
        return dataset

    def sample(self, k, tasks):
        clusters = []
        for i in range(k):
            r = random.randint(0,480188)
            fn = int(r/tasks)
            ln = r%tasks
            with open(f"data_part_{fn}.txt","r") as file:
                for _ in range(ln-1):
                    file.readline()
                pl = parse_line(file.readline())
            clusters.append(pl)
        return clusters

if __name__ == "__main__":
    # data_file = sys.argv[1]
    # k = int(sys.argv[2])
    fan = Fan("data1.csv", 3)
    fan.start()

# context = zmq.Context()
#
# # socket with workers
# workers = context.socket(zmq.PUSH)
# workers.bind("tcp://*:5557")
#
# # socket with sink
# sink = context.socket(zmq.PUSH)
# sink.connect("tcp://localhost:5558")
#
#
# print("Press enter when workers are ready...")
# _ = input()
# print("sending tasks to workers")
#
# sink.send(b'3,3')
#
# random.seed()
#
# a = [[1,2,2],[1,2,3],[4,2,5]]
# b = [[3,2,1],[8,2,1],[3,6,7]]
#
# for i in range(len(a)):
#     workers.send_json({"turn": i, "row":a[i], "b":b})
#     print("Enviado",i)
#
# c = mat_mul(a,b)
# print(c)
