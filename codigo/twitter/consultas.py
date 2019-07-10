import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

coleccion = mydb["csv1"]
db_usuarios = mydb["usuarios"]

papi = 0
lista = [[1,2,3,4], [5,6,7,8]]
for l in lista:
    if 1 in l:
        papi = lista.index(l)
        l.append(papi)

print(lista)