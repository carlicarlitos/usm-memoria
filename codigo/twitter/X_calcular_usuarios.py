import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]
coleccion_completa = mydb["csv_all"]
bd_usuarios = mydb["usuarios"]


#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)


usuarios = bd_usuarios.find({})
puntaje_por_id = {}
puntajes = []
lista_puntajes = []

for usuario in usuarios:
    puntaje_aux = 0
    userid = usuario["id"]
    seguidores = usuario["followers_count"] + 1
    amigos = usuario["friends_count"] + 1
    estados = usuario["statuses_count"] + 1
    puntaje_aux = ((seguidores-amigos)/(seguidores+amigos-abs(seguidores-amigos)))
    puntaje_por_id[userid] = puntaje_aux
    if puntaje_aux not in lista_puntajes:
        lista_puntajes.append(puntaje_aux)

lista_puntajes.sort()
print(len(lista_puntajes))
print(lista_puntajes)
