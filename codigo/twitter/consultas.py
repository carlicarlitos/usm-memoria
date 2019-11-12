import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time
import pandas as pd

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

topics = mydb["nuevos_topicos"]
hilos = mydb["nuevos_hilos"]

topicos = [topico for topico in topics.find({})]
nhilos = []
for topico in topicos:
    contador = 0
    hilos_por_topico = hilos.find({"topico" : topico["topico"]})
    for hilo in hilos_por_topico:
        contador+=1
    nhilos.append(contador)
it = 0
topicos_mostrar = []

print(nhilos)
while it <= 4:
    m = max(nhilos)
    maximos = []

    for ind, val in enumerate(nhilos):
        if val == m:
            maximos.append(ind)
            it+=1
            topicos_mostrar.append(ind)
            nhilos[ind] = 0
print(topicos_mostrar)

columnas_mantener = ["Topico " + str(topico) for topico in topicos_mostrar]
print(columnas_mantener)
lista_topicos = ["Topico " + str(topico["topico"]) for topico in topicos]
columnas_borrar = []

for columna in lista_topicos:
    if columna not in columnas_mantener:
        columnas_borrar.append(columna)

print(columnas_borrar)

palabras_topicos = [[palabra for palabra in topico["palabras"]] for topico in topicos]
borrar = ",".join(columnas_borrar)
df = pd.DataFrame(palabras_topicos).transpose()
df.columns = lista_topicos
df = df.drop(columnas_borrar, axis = 1)
df.to_csv(r'topicos.csv')

print(df.head())
