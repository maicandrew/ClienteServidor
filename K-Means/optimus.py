import numpy as np
from scipy.sparse import lil_matrix

users = {}
for i in range(1,5):
    with open(f"combined_data_{i}.txt", "r") as file:
        for line in file:
            line = line[:-1]
            if line[-1] != ":":
                user = int(line.split(",")[0])
                if not user in users:
                    user_id = len(users)
                    users[user] = user_id
                    print(f"Usuario {user} agregado con id: {user_id}")

ratings = [None]*len(users)
movie = 0
with open("data.txt", "r")as file:
    for line in file:
        line = line[:-1]
        if line[-1] == ":":
            movie = int(line[:-1])
            print(f"Pelicula {movie}")
        else:
            sp = line.split(",")
            user = int(sp[0])
            rate = int(sp[1])
            user_id = users[user]
            if not ratings[user_id]:
                ratings[user_id] = []
            ratings[user_id].append((movie, rate))
print(f"########################\n###Archivo {i} terminado###\n########################")
del users
with open("data.csv", "a") as f:
    for i in ratings:
        c = ""
        for u in i:
            c += f"({u[0]} {u[1]}),"
        c = c[:-1]+"\n"
        f.write(c)
print("Terminado")
