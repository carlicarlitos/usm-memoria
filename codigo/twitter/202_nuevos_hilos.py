import pymongo
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

tweets = coleccion_completa.find({})
primeros_tweets = coleccion_completa.find({"in_response_to" : None})

hilos_ref = []
all_hilos = []
hilos = []
for tweet in primeros_tweets:
    hilos.append([tweet])
for hilo in hilos:
    for tweet in hilo:
        inicio = tweet["id"]
        siguiente = coleccion_completa.find_one({"in_response_to" : inicio})
        if siguiente == None:
            continue
        hilo.append(siguiente)

num_hilo = 0
for hilo in hilos:
    print("hilo "+str(num_hilo))
    rts = 0
    favs = 0
    cant_tweets = 0
    textos = []
    hashtags_hilo = []
    hilo_aux = {}
    #conseguir valores
    for tweet_hilo in hilo:
        actualizar = coleccion_completa.update_one({"id" : tweet_hilo["id"]},{"$set" : {"hilo" : num_hilo, "pos_hilo" : cant_tweets}})
        rts += tweet_hilo["retweet_count"]
        favs += tweet_hilo["favorite_count"]
        hashtags_tweet = tweet_hilo["hashtags"]
        texto = " ".join(tweet_hilo["text"].split())
        fecha = tweet_hilo["created_at"]
        textos.append(texto)
        cant_tweets += 1
        user = tweet_hilo["user"]
        if len(hashtags_tweet) >=1:
            for hashtag in hashtags_tweet:
                if hashtag not in hashtags_hilo:
                    hashtags_hilo.append(hashtag)
    #armar hilo
    uid = user["id"]
    usuario = usuarios.find_one({"id": uid})
    sigmo = usuario["puntaje_sigmoideo"]
    hilo_aux["hilo"] = num_hilo
    hilo_aux["texto"] = " ".join(textos)
    hilo_aux["usuario"] =   
    hilo_aux["fecha"] = fecha
    hilo_aux["total_tweets"] = cant_tweets
    hilo_aux["total_retweet"] = rts
    hilo_aux["total_favorites"] = favs
    hilo_aux["total_hashtags"] = len(hashtags_hilo)
    hilo_aux["promedio_retweet"] = round(rts/cant_tweets)
    hilo_aux["promedio_favorites"] = round(favs/cant_tweets)
    hilo_aux["puntaje_sigmoideo"] = sigmo
    hilo_aux["all_hashtags"] = hashtags_hilo

    all_hilos.append(hilo_aux)
    num_hilo +=1
x = bd_hilos.insert_many(all_hilos)