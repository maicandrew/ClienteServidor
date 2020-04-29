import zmq
import pandas
from math import ceil
import time
import sys
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

class Fan:
    def __init__(self, data_file, k, sinkPull = "tcp://*:7557", sinkPush = "tcp://localhost:7556", workers = "tcp://*:7555", max_it = 1000, tasks = 10):
        self.dataset = self.read_data(data_file)
        self.k = k
        self.tasks = tasks
        self.max_it = max_it
        self.totalTasks = ceil(len(self.dataset)/self.tasks)
        self.context = zmq.Context()
        self.sinkPush = self.context.socket(zmq.PUSH)
        self.sinkPull = self.context.socket(zmq.PULL)
        self.workers = self.context.socket(zmq.PUSH)
        self.sinkPull.bind(sinkPull)
        self.sinkPush.connect(sinkPush)
        self.workers.bind(workers)
        self.clusters = random.sample(self.dataset, k)
        self.columns = len(self.clusters[0])

    def start(self):
        print("Press enter when workers and sink are ready...", end='')
        input()
        iteration = 0
        done = False
        new_clusters = deepcopy(self.clusters)
        while not done:
            dsink = {
                "tasks" : self.totalTasks,
                "clusters" : len(self.clusters)
            }
            self.sinkPush.send_json(dsink)
            for i in range(self.totalTasks):
                dworker = {
                    "clusters" : new_clusters,
                    "range" : [self.tasks*i,self.tasks*(i+1)]
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
        print("Terminado")
        print(self.clusters)

    def compareAssignment(self, new):
        for i,c in enumerate(self.clusters):
            for j in c:
                if new[i].get(j,None) != self.clusters[i][j]:
                    return False
        return True

    def show_assignments(self):
        df = self.dataset.iloc[:,-1].to_dict()
        class_dict = {}
        for i,c in enumerate(self.clusters):
            class_dict[f"cluster_{i}"] = dict()
            for s in c:
                try:
                    class_dict[f"cluster_{i}"][f"{df[s]}"] += 1
                except:
                    class_dict[f"cluster_{i}"][f"{df[s]}"] = 1
            print(class_dict)

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


if __name__ == "__main__":
    # data_file = sys.argv[1]
    # k = int(sys.argv[2])
    fan = Fan("test.txt", 3)
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
