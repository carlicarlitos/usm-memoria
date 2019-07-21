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
bd_users = mydb["nuevos_usuarios"]
bd_bi = mydb["nuevos_bi_hilos"]
bd_tweets_bi = mydb["nuevos_bi_tweets"] 
hilo_resto = mydb["nuevos_bi_hilo_resto"]
tweet_resto = mydb["nuevos_bi_tweet_resto"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

#datos
hilos = bd_hilos.find({})
all_hilos = []
muestra = []
resto = []
for hilo in hilos:
    all_hilos.append(hilo)

muestra = random.sample(all_hilos, 274)

for hilo in all_hilos:
    if hilo not in muestra:
        resto.append(hilo)

hilos_aux = []
ntweets = []
for hilo in muestra:
    #preparacion dic hilo
    hilo_aux = {}
    hashs = []
    emo_pres = []
    emo_media = []
    emo_moda = []
    emo_max = []
    n_hilo = hilo["hilo"]
    user_id = hilo["usuario"]["id"]
    fecha = hilo["fecha"]
    cant_tweets = hilo["total_tweets"]
    rt = hilo["total_retweet"]
    fav = hilo["total_favorites"]
    nhash = hilo["total_hashtags"]
    prt = hilo["promedio_retweet"]
    pfv = hilo["promedio_favorites"]
    psi = hilo["puntaje_sigmoideo"]
    hashs = hilo["all_hashtags"]
    emo_media = hilo["emocion_media"]
    emo_pres = hilo["emocion_presente"]
    emo_moda = hilo["emocion_moda"]
    emo_max = hilo["emocion_max"]
    topico = hilo["topico"]

    #atributos de emociones
    emo_max_1 = emo_max[0]
    emo_moda_1 = emo_moda[0]
    emo_media_1 = emo_media[0]
    emo_pres_1 = emo_pres[0]
    emo_max_2 = emo_max[1]
    emo_moda_2 = emo_moda[1]
    emo_media_2 = emo_media[1]
    emo_pres_2 = emo_pres[1]
    tupla_max = emo_max_1 + "+" + emo_max_2

    #atributos usuario
    usuario = bd_users.find_one({"id": user_id })
    user_follow = usuario["followers_count"]
    user_friend = usuario["friends_count"]
    veri = usuario["verified"]
    statuses = usuario["statuses_count"]
    puntaje_sigmo = usuario["puntaje_sigmoideo"]

    #armar hilo
    hilo_aux["hilo"] = n_hilo
    hilo_aux["fecha"] = fecha
    hilo_aux["topico"] = topico
    hilo_aux["total_tweets"] = cant_tweets
    hilo_aux["total_retweets"] = rt
    hilo_aux["total_favoritos"] = fav
    hilo_aux["total_hashtags"] = nhash
    hilo_aux["promedio_retweets"] = prt
    hilo_aux["promedio_favoritos"] = pfv
    hilo_aux["emocion_hilo_1"] = emo_max_1
    hilo_aux["tupla_emocion_hilo"] = tupla_max
    hilo_aux["usuario"] = user_id
    hilo_aux["user_total_followers"] = user_follow
    hilo_aux["user_total_friends"] = user_friend
    hilo_aux["user_total_status"] = statuses
    hilo_aux["user_puntaje_sigmo"] = psi
    hilo_aux["user_verificado"] = veri
    
    #Guardar hilos
    hilos_aux.append(hilo_aux)

    #conseguir tweets
    tweets = coleccion_completa.find({"hilo" : n_hilo})
    for tweet in tweets:
        ntweet = {}
        tid = tweet["id"]
        uid = user_id
        fecha = tweet["created_at"]
        psigmo = tweet["puntaje_sigmoideo"]
        trt = tweet["retweet_count"]
        tfv = tweet["favorite_count"]
        thc = tweet["hashtag_count"]
        art = tweet["aporte_retweet"]
        afv = tweet["aporte_favoritos"]
        thilo = tweet["hilo"]
        tpos = tweet["pos_hilo"]
        philo = tweet["progreso_hilo"]
        temocion = tweet["emocion"]

        ntweet["hilo"] = thilo
        ntweet["posicion_hilo"] = tpos
        ntweet["fecha"] = fecha
        ntweet["id"] =  tid
        ntweet["user_id"] = uid
        ntweet["emocion"] = temocion
        ntweet["puntaje_sigmoideo"] = psigmo
        ntweet["total_rt"] = trt
        ntweet["total_fav"] = tfv
        ntweet["total_hashtags"] = thc
        ntweet["aporte_rt"] = art
        ntweet["aporte_fav"] = afv
        ntweet["progreso_hilo"] = philo

        ntweets.append(ntweet)


x = bd_bi.insert_many(hilos_aux)
y = bd_tweets_bi.insert_many(ntweets)


hilos_aux = []
ntweets = []
for hilo in resto:
    #preparacion dic hilo
    hilo_aux = {}
    hashs = []
    emo_pres = []
    emo_media = []
    emo_moda = []
    emo_max = []
    n_hilo = hilo["hilo"]
    user_id = hilo["usuario"]["id"]
    fecha = hilo["fecha"]
    cant_tweets = hilo["total_tweets"]
    rt = hilo["total_retweet"]
    fav = hilo["total_favorites"]
    nhash = hilo["total_hashtags"]
    prt = hilo["promedio_retweet"]
    pfv = hilo["promedio_favorites"]
    psi = hilo["puntaje_sigmoideo"]
    hashs = hilo["all_hashtags"]
    emo_media = hilo["emocion_media"]
    emo_pres = hilo["emocion_presente"]
    emo_moda = hilo["emocion_moda"]
    emo_max = hilo["emocion_max"]
    topico = hilo["topico"]

    #atributos de emociones
    emo_max_1 = emo_max[0]
    emo_moda_1 = emo_moda[0]
    emo_media_1 = emo_media[0]
    emo_pres_1 = emo_pres[0]
    emo_max_2 = emo_max[1]
    emo_moda_2 = emo_moda[1]
    emo_media_2 = emo_media[1]
    emo_pres_2 = emo_pres[1]
    tupla_max = emo_max_1 + "+" + emo_max_2

    #atributos usuario
    usuario = bd_users.find_one({"id": user_id })
    user_follow = usuario["followers_count"]
    user_friend = usuario["friends_count"]
    veri = usuario["verified"]
    statuses = usuario["statuses_count"]
    puntaje_sigmo = usuario["puntaje_sigmoideo"]

    #armar hilo
    hilo_aux["hilo"] = n_hilo
    hilo_aux["fecha"] = fecha
    hilo_aux["topico"] = topico
    hilo_aux["total_tweets"] = cant_tweets
    hilo_aux["total_retweets"] = rt
    hilo_aux["total_favoritos"] = fav
    hilo_aux["total_hashtags"] = nhash
    hilo_aux["promedio_retweets"] = prt
    hilo_aux["promedio_favoritos"] = pfv
    hilo_aux["emocion_hilo_1"] = emo_max_1
    hilo_aux["tupla_emocion_hilo"] = tupla_max
    hilo_aux["usuario"] = user_id
    hilo_aux["user_total_followers"] = user_follow
    hilo_aux["user_total_friends"] = user_friend
    hilo_aux["user_total_status"] = statuses
    hilo_aux["user_puntaje_sigmo"] = psi
    hilo_aux["user_verificado"] = veri
    
    #Guardar hilos
    hilos_aux.append(hilo_aux)

    #conseguir tweets
    tweets = coleccion_completa.find({"hilo" : n_hilo})
    for tweet in tweets:
        ntweet = {}
        tid = tweet["id"]
        uid = user_id
        fecha = tweet["created_at"]
        psigmo = tweet["puntaje_sigmoideo"]
        trt = tweet["retweet_count"]
        tfv = tweet["favorite_count"]
        thc = tweet["hashtag_count"]
        art = tweet["aporte_retweet"]
        afv = tweet["aporte_favoritos"]
        thilo = tweet["hilo"]
        tpos = tweet["pos_hilo"]
        philo = tweet["progreso_hilo"]
        temocion = tweet["emocion"]

        ntweet["hilo"] = thilo
        ntweet["posicion_hilo"] = tpos
        ntweet["fecha"] = fecha
        ntweet["id"] =  tid
        ntweet["user_id"] = uid
        ntweet["emocion"] = temocion
        ntweet["puntaje_sigmoideo"] = psigmo
        ntweet["total_rt"] = trt
        ntweet["total_fav"] = tfv
        ntweet["total_hashtags"] = thc
        ntweet["aporte_rt"] = art
        ntweet["aporte_fav"] = afv
        ntweet["progreso_hilo"] = philo

        ntweets.append(ntweet)


x = hilo_resto.insert_many(hilos_aux)
y = tweet_resto.insert_many(ntweets)