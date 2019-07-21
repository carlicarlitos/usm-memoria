import pymongo
from bson.objectid import ObjectId
import os
import random
from twython import Twython
import pandas as pd
import time
import numpy as np

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]
coleccion_completa = mydb["nuevos_tweets"]
bd_hilos = mydb["nuevos_hilos"]
bd_users = mydb["nuevos_usuarios"]
bd_bi = mydb["nuevos_bi_hilos"]
bd_tweets_bi = mydb["nuevos_bi_tweets"] 
hilo_resto = mydb["nuevos_bi_hilo_resto"]
tweet_resto = mydb["nuevos_bi_tweet_resto"]

hilos_orange = mydb["orange_hilos"]
tweet_orange = mydb["orange_tweet"] 
hresto_orange = mydb["orange_hresto"]
tresto_orange = mydb["orange_tresto"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

#datos

muestra = bd_bi.find({})
resto = hilo_resto.find({})
datasets = [[muestra,[hilos_orange, tweet_orange]],[resto, [hresto_orange,tresto_orange]]]


for dataset in datasets:
    hilos_aux = []
    ntweets = []
    cursor_hilos = dataset[0]
    colecciones = dataset[1]
    for hilo in cursor_hilos:
        #preparacion dic hilo
        hilo_aux = {}
        hashs = []
        emo_max = []

        n_hilo = hilo["hilo"]
        cant_tweets = hilo["total_tweets"]
        rt = hilo["total_retweets"]
        fav = hilo["total_favoritos"]
        nhash = hilo["total_hashtags"]
        prt = hilo["promedio_retweets"]
        pfv = hilo["promedio_favoritos"]
        psi = hilo["user_puntaje_sigmo"]
        topico = hilo["topico"]
        user_id = hilo["usuario"]

        #atributos de emociones
        emo_max_1 = hilo["emocion_hilo_1"]
        tupla_max = hilo["tupla_emocion_hilo"]

        #atributos usuario
        usuario = bd_users.find_one({"id": user_id })
        user_follow = usuario["followers_count"]
        veri = usuario["verified"]

        #armar hilo
        hilo_aux["hilo"] = n_hilo
        hilo_aux["topico"] = topico
        hilo_aux["total_tweets"] = cant_tweets
        hilo_aux["total_retweets"] = rt
        hilo_aux["total_favoritos"] = fav
        hilo_aux["promedio_rt"] = prt
        hilo_aux["promedio_fv"] = pfv
        hilo_aux["total_hashtags"] = nhash
        hilo_aux["emocion_hilo_1"] = emo_max_1
        hilo_aux["tupla_emocion_hilo"] = tupla_max
        hilo_aux["user_total_followers"] = user_follow
        hilo_aux["user_puntaje_sigmo"] = psi
        hilo_aux["user_verificado"] = veri
        
        hilo_aux["log_prt"] = np.log10(prt+1)
        hilo_aux["log_pfv"] = np.log10(pfv+1)        
        hilo_aux["log_total_retweets"] = np.log10(rt+1)
        hilo_aux["log_total_favoritos"] = np.log10(fav+1)
        hilo_aux["rnd_user_puntaje_sigmo"] = round(psi,1)
        hilo_aux["log_user_total_followers"] = np.log10(user_follow+1)

        #Guardar hilos
        hilos_aux.append(hilo_aux)

        #conseguir tweets
        tweets = coleccion_completa.find({"hilo" : n_hilo})
        for tweet in tweets:
            #datos tweet
            ntweet = {}
            tid = tweet["id"]
            trt = tweet["retweet_count"]
            tfv = tweet["favorite_count"]
            thc = tweet["hashtag_count"]
            art = tweet["aporte_retweet"]
            afv = tweet["aporte_favoritos"]
            thilo = tweet["hilo"]
            tpos = tweet["pos_hilo"]
            philo = tweet["progreso_hilo"]
            temocion = tweet["emocion"]
            #armar dict tweet
            ntweet["hilo"] = thilo
            ntweet["posicion_hilo"] = tpos
            ntweet["id"] =  tid
            ntweet["emocion"] = temocion
            ntweet["puntaje_sigmoideo"] = psi
            ntweet["total_rt"] = trt
            ntweet["total_fav"] = tfv
            ntweet["total_hashtags"] = thc
            ntweet["aporte_rt"] = art
            ntweet["aporte_fav"] = afv
            ntweet["progreso_hilo"] = philo
            ntweet["user_puntaje_sigmo"] = psi
            ntweet["user_verificado"] = veri
            #Calculos
            ntweet["log_total_rt"] = np.log10(trt+1)
            ntweet["log_total_fav"] = np.log10(tfv+1)
            ntweet["rnd_user_puntaje_sigmo"] = round(psi,1)
            ntweet["log_user_total_followers"] = np.log10(user_follow+1)
            #guardar tweets
            ntweets.append(ntweet)

    dx = colecciones[0].drop()
    dy = colecciones[1].drop()
    x = colecciones[0].insert_many(hilos_aux)
    y = colecciones[1].insert_many(ntweets)

