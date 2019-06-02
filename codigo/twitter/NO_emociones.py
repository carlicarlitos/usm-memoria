import os
import random
import pymongo
import time
import pandas as pd
from twython import Twython

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]

csv_all = mydb["csv_all"]



#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

tweets = csv_all.find({})

hilos_ref = []
all_hilos = []

for tweet in tweets[:1]:
    if tweet["hilo_ref"] not in hilos_ref:
        hilos_ref.append(tweet["hilo_ref"])

for hilo in hilos_ref[:1]:
    tweets_hilo = coleccion_completa.find({"hilo_ref": hilo})
    lista_aux_hilos = []
    csv = ""
    for tweet_aux in tweets_hilo:
        lista_aux_hilos.append(tweet_aux["text"])
        csv = tweet_aux["csv"]
        hilo_csv = tweet["hilo"]
    print(csv)
    print(hilo)
    print(lista_aux_hilos)

