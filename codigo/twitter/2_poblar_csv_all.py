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
n_hilo_ref = 1


#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

#Almacenar hilo csv1
coleccion = mydb["csv1"]
hilos = coleccion.distinct("hilo")
documents = coleccion.find({})
dic_hilo = {}

for hilo in hilos:
    dic_hilo[hilo] = n_hilo_ref
    n_hilo_ref+=1

documents_2 = []
for document in documents:
    document["hilo_ref"] = dic_hilo[document["hilo"]]
    documents_2.append(document)

    myquery = { "id": document["id"] }
    newvalues = { "$set": { "hilo_ref": dic_hilo[document["hilo"]] } }
    agregar_hilo_ref = coleccion.update_one(myquery, newvalues)

guardar_prueba = coleccion_completa.insert_many(documents_2)

#Almacenar hilo csv2
coleccion = mydb["csv2"]
hilos = coleccion.distinct("hilo")
documents = coleccion.find({})
dic_hilo = {}

for hilo in hilos:
    dic_hilo[hilo] = n_hilo_ref
    n_hilo_ref+=1

documents_2 = []
for document in documents:
    document["hilo_ref"] = dic_hilo[document["hilo"]]
    documents_2.append(document)

    myquery = { "id": document["id"] }
    newvalues = { "$set": { "hilo_ref": dic_hilo[document["hilo"]] } }
    agregar_hilo_ref = coleccion.update_one(myquery, newvalues)

guardar_prueba = coleccion_completa.insert_many(documents_2)

#Almacenar hilo csv3
coleccion = mydb["csv3"]
hilos = coleccion.distinct("hilo")
documents = coleccion.find({})
dic_hilo = {}

for hilo in hilos:
    dic_hilo[hilo] = n_hilo_ref
    n_hilo_ref+=1

documents_2 = []
for document in documents:
    document["hilo_ref"] = dic_hilo[document["hilo"]]
    documents_2.append(document)

    myquery = { "id": document["id"] }
    newvalues = { "$set": { "hilo_ref": dic_hilo[document["hilo"]] } }
    agregar_hilo_ref = coleccion.update_one(myquery, newvalues)

guardar_prueba = coleccion_completa.insert_many(documents_2)

#Almacenar hilo csv4
coleccion = mydb["csv4"]
hilos = coleccion.distinct("hilo")
documents = coleccion.find({})
dic_hilo = {}

for hilo in hilos:
    dic_hilo[hilo] = n_hilo_ref
    n_hilo_ref+=1

documents_2 = []
for document in documents:
    document["hilo_ref"] = dic_hilo[document["hilo"]]
    documents_2.append(document)

    myquery = { "id": document["id"] }
    newvalues = { "$set": { "hilo_ref": dic_hilo[document["hilo"]] } }
    agregar_hilo_ref = coleccion.update_one(myquery, newvalues)

guardar_prueba = coleccion_completa.insert_many(documents_2)

#Almacenar hilo csv5
coleccion = mydb["csv5"]
hilos = coleccion.distinct("hilo")
documents = coleccion.find({})
dic_hilo = {}

for hilo in hilos:
    dic_hilo[hilo] = n_hilo_ref
    n_hilo_ref+=1

documents_2 = []
for document in documents:
    document["hilo_ref"] = dic_hilo[document["hilo"]]
    documents_2.append(document)

    myquery = { "id": document["id"] }
    newvalues = { "$set": { "hilo_ref": dic_hilo[document["hilo"]] } }
    agregar_hilo_ref = coleccion.update_one(myquery, newvalues)

guardar_prueba = coleccion_completa.insert_many(documents_2)