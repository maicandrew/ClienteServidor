import zmq
import difflib as dl
from math import ceil, sqrt, acos
import time
import sys
import os
from copy import deepcopy
import random
import json
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

def rln(r,fl,tasks):
    fn = int(r/tasks)
    ln = r%tasks
    with open(f"{fl}{fn}.txt","r") as file:
        for _ in range(ln-1):
            file.readline()
        pl = parse_line(file.readline())
    return pl

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

class Fan:
    def __init__(self, data_file, k, sinkPull = "tcp://*:7557", sinkPush = "tcp://localhost:7556", workers = "tcp://*:7555", max_it = 10, tasks = 100):
        self.dataset = data_file
        self.tol = 0.25
        self.tot_mov = 0
        self.k = k
        self.tasks = tasks
        self.max_it = max_it
        self.clusters = self.sample(k,tasks)
        self.totalTasks = count_f(data_file)
        self.context = zmq.Context()
        self.sinkPush = self.context.socket(zmq.PUSH)
        self.sinkPull = self.context.socket(zmq.PULL)
        self.workers = self.context.socket(zmq.PUSH)
        self.sinkPull.bind(sinkPull)
        self.sinkPush.connect(sinkPush)
        self.workers.bind(workers)

    def start(self):
        iteration = 0
        done = False
        init_time = time.time()
        inertia = 0
        while not done:
            st = time.time()
            print(f"Iteration: {iteration+1}")
            dsink = {
                "file" : "new_clusters.txt",
                "tasks" : self.tasks,
                "Totaltasks" : self.totalTasks,
                "clusters" : self.k
            }
            self.sinkPush.send_json(dsink)
            for i in range(self.totalTasks):
                dworker = {
                    "tasks" : self.tasks,
                    "clusters" : self.clusters,
                    "data_file":self.dataset,
                    "ifile" : i,
                    "it":iteration
                }
                self.workers.send_json(dworker)
            res = self.sinkPull.recv_json()
            new_clusters = res["clusters"]
            inertia = res["inertia"]
            print("inertia:",inertia)
            if self.compareAssignment(new_clusters):
                done = True
            elif iteration >= self.max_it:
                print("Limite de iteraciones")
                break
            else:
                with open(self.clusters,"w") as f:
                    with open(new_clusters,"r") as cl_f:
                        data = json.load(cl_f)
                    json.dump(data,f)
                iteration += 1
            et = time.time()
            print("Time:",et-st)
        end_time = time.time()
        with open(f"clusters_for_{self.k}","w") as f:
            with open(new_clusters,"r") as cl_f:
                data = json.load(cl_f)
            json.dump(data,f)
        with open("results.txt","r+") as result_file:
            try:
                data_charge = json.load(result_file)
            except:
                data_charge = {}
            data_charge[self.k] = {"k":self.k, "inertia":inertia, "sum":self.tot_mov,"iterations":iteration, "time": end_time-init_time}
            result_file.seek(0)
            json.dump(data_charge,result_file)
        print("Terminado:",end_time-init_time)
        return inertia

    def css(self, p1, p2, n1, n2):
        com = None
        if len(p1) <= len(p2):
            com = p1
        else:
            com = p2
        dot = 0
        for key in com:
            dot += float(p1.get(key,0)) * float(p2.get(key,0))
        cos_sim = dot / (n1 * n2)
        if -1.0<=cos_sim<=1.0:
            return cos_sim
        else:
            return float(int(cos_sim))

    def read_cl(self, file_name):
        with open(file_name,"r") as cl_f:
            data = json.load(cl_f)
            clusters = data["clusters"]
            norms = data["norms"]
        return clusters, norms

    def compareAssignment(self, new):
        print("Comparando clusters...")
        new_cl, norm_n = self.read_cl(new)
        flag = False
        for i in range(len(norm_n)):
            if norm_n[i] == 0:
                new_cl[i] = rln(random.randint(0,470000),self.dataset,self.tasks)
                flag = True
                print(f"cluster {i} con norma 0")
        if flag:
            flag = False
            norm_n = norm(new_cl)
            with open(new,"w") as cl_f:
                json.dump({"clusters":new_cl,"norms":norm_n},cl_f)
            return False
        clusters, norm_a = self.read_cl(self.clusters)
        tot_mov = 0
        for i in range(len(new_cl)):
            print("cluster:",i)
            mov = acos(self.css(new_cl[i], clusters[i], norm_a[i],norm_n[i]))
            tot_mov += mov
        print(tot_mov)
        self.tot_mov = tot_mov
        if tot_mov > self.tol*self.k:
            return False
        else:
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

    def kill(self):
        self.sinkPull.close()
        self.workers.close()
        self.context.destroy()

    def sample(self, k, tasks):
        file_name = "actual_clusters"
        clusters = []
        ls = []
        for _ in range(k):
            r = random.randint(0,470000)
            while r in ls:
                r = random.randint(0,470000)
            ls.append(r)
            pl = rln(r,self.dataset,tasks)
            clusters.append(pl)
        norm_c = norm(clusters)
        with open(file_name,"w") as cl_f:
            json.dump({"clusters":clusters,"norms":norm_c},cl_f)
        return file_name

if __name__ == "__main__":
    # data_file = sys.argv[1]
    # k = int(sys.argv[2])
    ks = [2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,150,200]
    for k in ks:
        fan = Fan("data_1_", k)
        fan.start()
        fan.kill()

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
