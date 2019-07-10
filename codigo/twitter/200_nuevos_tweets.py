import pymongo
from bson.objectid import ObjectId
import os
import random
from twython import Twython
import pandas as pd
import time
from datetime import datetime

def cortar_lista(lista, tamanno):
    chunk_size = tamanno
    L = lista
    L_nueva = []
    for start in range(0,len(L),chunk_size):
        end = start + chunk_size
        trozo = L[start:end]
        L_nueva.append(trozo)
    return L_nueva

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

#Tweets archivos
lista_aux_1 = []
lista_aux_1 = [line.rstrip('\n') for line in open("hilos.txt", "r")]
set_lista = set(lista_aux_1)
lista_aux = list(set_lista)

bd_all = mydb["nuevos_tweets"]
consultas = 0

print("inicio hilos")

lista_hilos = []
for hilo in lista_aux:
    lista_hilos.append([hilo])

id_all_tweets = []
id_all_tweets = cortar_lista(lista_aux, 100)
hilos = {}
n_hilos = 0
mas_tweets = []
n_consultas = 0
limite_ocupado = 1
sobrantes = 0
for tweets_id_search in id_all_tweets:
    #Comprobar si se pueden hacer consultas
    tweets = {}
    tweets_limpios = []
    if sobrantes == 0:
        limite_ocupado = 1
    while limite_ocupado == 1:
        if n_consultas == 180:
            print("esperando consulta estados")
            time.sleep(900)
            n_consultas = 1
        print("consultando estado")
        consulta_estado = twitter.get_application_rate_limit_status(resources = "statuses")
        n_consultas +=1
        estado = consulta_estado["resources"]
        estatuses = estado["statuses"]
        estado_lookup = estatuses["/statuses/lookup"]
        sobrantes = estado_lookup["remaining"]
        if sobrantes < 1:
            print("esperando un minuto")
            time.sleep(66)
        elif sobrantes == 180:
            n_consultas = 0
        else:
            limite_ocupado = 0
    #Conseguir tweets de la iteracion
    tweets = twitter.lookup_status(id=tweets_id_search, tweet_mode = "extended")
    consultas+=1 
    sobrantes -= 1
    print("numero de consultas: "+ str(consultas))
    #Limpiar tweets
    mas_tweets = []
    for tweet in tweets:

        tweet_aux = {}
        user_aux_1 = {}
        user_aux = {}
        id_tweet = tweet["id"]

        id_papi = tweet["in_reply_to_status_id"]
        if id_papi != "null":
            mas_tweets.append(id_papi)

        user_aux = tweet["user"]
        user_aux_1["id"] = user_aux["id"]
        user_aux_1["id_str"] = user_aux["id_str"]
        user_aux_1["name"] = user_aux["name"]
        user_aux_1["followers_count"] = user_aux["followers_count"]
        user_aux_1["friends_count"] = user_aux["friends_count"]
        
        user_created_at = user_aux["created_at"]
        udt = datetime.strptime(user_created_at, '%a %b %d %H:%M:%S +0000 %Y')
        user_aux_1["created_at"] = udt
        user_aux_1["verified"] = user_aux["verified"]
        user_aux_1["statuses_count"] = user_aux["statuses_count"]
        
        created_at = tweet["created_at"]
        dt = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
        #crear tweet con solo datos utiles
#        tweet_aux["hilo"] = hilo
        tweet_aux["created_at"] = dt
        tweet_aux["id"] = tweet["id"]
        tweet_aux["id_str"] = tweet["id_str"]
        tweet_aux["in_response_to"] = id_papi
        tweet_aux["text"] = tweet["full_text"]
        tweet_aux["user"] = user_aux_1
        tweet_aux["retweet_count"] = tweet["retweet_count"]
        tweet_aux["favorite_count"] = tweet["favorite_count"]
        #hashtags
        hashtags = []
        hash_aux = []
        hashtags = tweet["entities"]["hashtags"]
        for hashtag in hashtags:
            hash_aux.append(hashtag["text"])
        tweet_aux["hashtag_count"] = len(hash_aux)
        tweet_aux["hashtags"] = hash_aux
        

        tweets_limpios.append(tweet_aux)
    #meter tweets limpios
    if len(tweets_limpios)>1:
        x = bd_all.insert_many(tweets_limpios)
    if len(mas_tweets)>0:
        id_all_tweets.append(mas_tweets)

tweets = bd_all.find({})
all_id = []

for tweet in tweets:
    tid = tweet["id"]
    mid = tweet["_id"]
    if tid not in all_id:
        all_id.append(tid)
    else:
        print("replicado")
        result = bd_all.delete_one({'_id': ObjectId(mid)})

