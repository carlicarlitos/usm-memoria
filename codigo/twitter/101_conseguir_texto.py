import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time
import dicttoxml



THIS_FOLDER = os.getcwd()

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]
coleccion_completa = mydb["csv_all"]
bd_hilos = mydb["hilos"]
bd_users = mydb["usuarios"]
bd_bi = mydb["hilos_bi"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

hilos = bd_bi.find({})

for hilo in hilos:
    n_hilo = hilo["hilo"]
    tweets = coleccion_completa.find({"hilo_ref" : n_hilo})
    string = " \n "
    tweets_hilos = []
    tweets_hilos_ordenados = []
    for tweet in tweets:
        tweet_aux = {}
        texto = tweet["text"]
        tid = tweet["id"]
        tweet_aux["id"] = tid
        tweet_aux["texto"] = texto
        tweets_hilos.append(tweet_aux)
    tweets_hilos_ordenados = sorted(tweets_hilos, key=lambda k: k['id']) 
    
    textos = []
    for estado in tweets_hilos_ordenados:
        texto = estado["texto"]
        texto_hilo = " ".join(texto.split())  
        textos.append(texto_hilo)
    texto_hilo_aux = string.join(textos)

    carpeta_guardar = "documentos"
    camino = os.path.join(THIS_FOLDER, carpeta_guardar)
    titulo = "\\hilo_" + str(n_hilo) + ".txt"
    nombre_archivo = camino + titulo

    archivo = open(nombre_archivo, "w+", encoding = "utf-8")
    archivo.write(texto_hilo_aux)
    archivo.close()
    

