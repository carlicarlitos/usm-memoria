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
bd_hilos = mydb["hilos"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)


tweets = coleccion_completa.find({})

hilos_ref = []
all_hilos = []
for tweet in tweets:
    if tweet["hilo_ref"] not in hilos_ref:
        hilos_ref.append(tweet["hilo_ref"])

for hilo in hilos_ref:
    rts = 0
    favs = 0
    replies = 0
    cant_tweets = 0
    hashtags_hilo = []
    tweets_hilo = coleccion_completa.find({"hilo_ref": hilo })
    hilo_aux = {}
    #conseguir valores
    for tweet_hilo in tweets_hilo:
        rts += tweet_hilo["retweet_count"]
        favs += tweet_hilo["favorite_count"]
        replies += tweet_hilo["replies"]
        hashtags_tweet = tweet_hilo["hashtags"]
        cant_tweets += 1
        csv = tweet_hilo["csv"] 
        hilo_csv = tweet_hilo["hilo"] 
        user = tweet_hilo["user"]
        if len(hashtags_tweet) >=1:
            for hashtag in hashtags_tweet:
                if hashtag not in hashtags_hilo:
                    hashtags_hilo.append(hashtag)
    #armar hilo
    hilo_aux["hilo"] = hilo
    hilo_aux["csv"] = csv
    hilo_aux["hilo_csv"] = hilo_csv
    hilo_aux["usuario"] = user
    hilo_aux["total_tweets"] = cant_tweets
    hilo_aux["total_retweet"] = rts
    hilo_aux["total_favorites"] = favs
    hilo_aux["total_replies"] = replies
    hilo_aux["all_hashtags"] = hashtags_hilo
    all_hilos.append(hilo_aux)


x = bd_hilos.insert_many(all_hilos)