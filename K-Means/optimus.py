import json

"""users = {}
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
print("Guardando")
with open('users.json', 'w+') as fp:
    json.dump(users, fp)
print("Terminado")"""
with open("users.json", "r") as fu:
    users = json.load(fu)
print("Usuarios cargados")
ratings = [None for _ in range(len(users))]
movie = 0
for i in range(1,5):
    with open(f"combined_data_{i}.txt", "r") as file:
        for line in file:
            line = line[:-1]
            if line[-1] == ":":
                movie = int(line[:-1])
                print(f"\rPelicula {movie}", end="")
            else:
                sp = line.split(",")
                user = sp[0]
                rate = int(sp[1])
                user_id = users[user]
                if not ratings[user_id]:
                    ratings[user_id] = {}
                ratings[user_id][movie] = rate
    print("")
    print(f"########################\n###Archivo {i} terminado###\n########################")

del users
contador = 0
for us in ratings:
    if contador % 100 == 0:
        if "f" in locals():
            f.close()
        f = open(f"data_part_{int(contador/100)}.txt", "w+")
    c = ""
    for m in us:
        c += f"({m} {us[m]}),"
    c = c[:-1]+"\n"
    f.write(c)
    print(f"\rLinea {contador}",end="")
    contador += 1
print("\nTerminado")
