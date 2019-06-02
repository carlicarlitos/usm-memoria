import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time

def cortar_lista(lista, tamanno):
    chunk_size = tamanno
    L = lista
    L_nueva = []
    # iterate over L in steps of 3
    for start in range(0,len(L),chunk_size): # xrange() in 2.x; range() in 3.x
        end = start + chunk_size
        trozo = L[start:end] # three-item chunks
        L_nueva.append(trozo)
    return L_nueva


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


usuarios = coleccion_completa.distinct("user.id")

print(type(usuarios))
print(len(usuarios))

id_all_users = cortar_lista(usuarios, 100)
usuarios_limpios = []

for user_id_search in id_all_users:
    users = twitter.lookup_user(user_id=user_id_search)
    for user_aux in users:
        user_aux_1 = {}
        user_aux_1["id"] = user_aux["id"]
        user_aux_1["id_str"] = user_aux["id_str"]
        user_aux_1["name"] = user_aux["name"]
        user_aux_1["followers_count"] = user_aux["followers_count"]
        user_aux_1["friends_count"] = user_aux["friends_count"]
        user_aux_1["created_at"] = user_aux["created_at"]
        user_aux_1["verified"] = user_aux["verified"]
        user_aux_1["statuses_count"] = user_aux["statuses_count"]
        usuarios_limpios.append(user_aux_1)

x = bd_usuarios.insert_many(usuarios_limpios)
    

