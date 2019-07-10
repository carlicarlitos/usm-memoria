import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time
from dicttoxml import dicttoxml

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

hilos = bd_hilos.find({})


hilos_aux = []
for hilo in hilos:
    #preparacion dic hilo
    hilo_aux = {}
    hashs = []
    emo_pres = []
    emo_media = []
    emo_moda = []
    emo_max = []
    n_hilo = hilo["hilo"]
    user_id = hilo["usuario"]["id"]
    cant_tweets = hilo["total_tweets"]
    rt = hilo["total_retweet"]
    fav = hilo["total_favorites"]
    rep = hilo["total_replies"]
    hashs = hilo["all_hashtags"]
    emo_max = hilo["Emocion_Max"]
    emo_moda = hilo["Emocion_Moda"]
    emo_media = hilo["Emocion_Media"]
    emo_pres = hilo["Emocion_Presente"]

    #atributos de emociones
    emo_max_1 = emo_max[0]
    emo_moda_1 = emo_moda[0]
    emo_media_1 = emo_media[0]
    emo_pres_1 = emo_pres[0]
    emo_max_2 = emo_max[1]
    emo_moda_2 = emo_moda[1]
    emo_media_2 = emo_media[1]
    emo_pres_2 = emo_pres[1]
    cant_hash = len(hashs)

    #atributos usuario
    usuario = bd_users.find_one({"id": user_id })

    user_follow = usuario["followers_count"]
    user_friend = usuario["friends_count"]
    veri = usuario["verified"]
    statuses = usuario["statuses_count"]
    puntaje_sigmo = usuario["puntaje_sigmo"]

    #calcular promedios
    promedio_rt = rt/cant_tweets
    promedio_fv = fav/cant_tweets
    promedio_rp = rep/cant_tweets

    #armar hilo
    hilo_aux["hilo"] = n_hilo
    hilo_aux["total_tweets"] = cant_tweets
    hilo_aux["total_retweets"] = rt
    hilo_aux["total_favoritos"] = fav
    hilo_aux["total_replies"] = rep
    hilo_aux["total_hashtags"] = cant_hash
    hilo_aux["promedio_retweets"] = promedio_rt
    hilo_aux["promedio_favoritos"] = promedio_fv
    hilo_aux["promedio_replies"] = promedio_rp
    hilo_aux["emocion_max_1"] = emo_max_1
    hilo_aux["emocion_max_2"] = emo_max_2
    hilo_aux["emocion_moda_1"] = emo_moda_1
    hilo_aux["emocion_moda_2"] = emo_moda_2
    hilo_aux["emocion_media_1"] = emo_media_1
    hilo_aux["emocion_media_2"] = emo_media_2
    hilo_aux["emocion_presente_1"] = emo_pres_1
    hilo_aux["emocion_presente_2"] = emo_pres_2
    hilo_aux["usuario"] = user_id
    hilo_aux["user_total_followers"] = user_follow
    hilo_aux["user_total_friends"] = user_friend
    hilo_aux["user_total_status"] = statuses
    hilo_aux["user_puntaje_sigmo"] = puntaje_sigmo
    hilo_aux["user_verificado"] = veri
    
    hilos_aux.append(hilo_aux)

x = bd_bi.insert_many(hilos_aux)