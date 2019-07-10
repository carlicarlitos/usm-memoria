import pymongo
from bson.objectid import ObjectId
import os
import random
from twython import Twython
import pandas as pd
import time

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]
coleccion_completa = mydb["nuevos_tweets"]
bd_hilos = mydb["nuevos_hilos"]
usuarios = mydb["nuevos_usuarios"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

#datos
tweets = coleccion_completa.find({})
hilos = bd_hilos.find({})

for hilo in hilos:
    n_hilo = hilo["hilo"]
    cant_tweets = hilo["total_tweets"]
    cant_favs = hilo["total_favorites"]
    cant_rt = hilo["total_retweet"]
    tweets = coleccion_completa.find({"hilo":n_hilo})
    sigmo = hilo["puntaje_sigmoideo"]

    for tweet in tweets:
        tid = tweet["id"]
        pos_tweet = tweet["pos_hilo"]
        cant_favs_tweet = tweet["favorite_count"]
        cant_rt_tweet = tweet["retweet_count"]
        progreso = round((99/(cant_tweets-1))*(pos_tweet)+1)
        aporte_fav = round(cant_favs_tweet/(cant_favs+1),3)
        aporte_rt = round(cant_rt_tweet/(cant_rt+1),3)
        actualizar = coleccion_completa.update_one({"id" : tid},{"$set" : {"puntaje_sigmoideo" : sigmo, "progreso_hilo" : progreso, "aporte_favoritos": aporte_fav, "aporte_retweet" : aporte_rt}})
