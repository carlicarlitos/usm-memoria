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

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

#Tweets archivos
csv1 = pd.read_csv('five_ten.csv', encoding='iso-8859-1')
csv2 = pd.read_csv('ten_fifteen.csv', encoding='iso-8859-1')
csv3 = pd.read_csv('fifteen_twenty.csv', encoding='iso-8859-1')
csv4 = pd.read_csv('twenty_twentyfive.csv', encoding='iso-8859-1')
csv5 = pd.read_csv('twentyfive_thirty.csv', encoding='iso-8859-1')

bd_all = mydb["csv_all"]
consultas = 0
#csv1
print("inicio csv1")
csv_origen = "csv1"
csv_aux = csv1
bd_prueba = mydb["csv1"]

lista_aux = []
lista_aux = csv_aux["id"].tolist()
id_all_tweets = []
id_all_tweets = cortar_lista(lista_aux, 100)

for tweets_id_search in id_all_tweets:
    
    #Comprobar si se pueden hacer consultas
    tweets = {}
    tweets_limpios = []
    limite_ocupado = 1
    while limite_ocupado == 1:
        consulta_estado = twitter.get_application_rate_limit_status(resources = "statuses")
        estado = consulta_estado["resources"]
        estatuses = estado["statuses"]
        estado_lookup = estatuses["/statuses/lookup"]
        sobrantes = estado_lookup["remaining"]
        if sobrantes < 1:
            time.sleep(60)
            print("esperando un minuto")
        else:
            limite_ocupado = 0
    #Conseguir tweets de la iteracion
    tweets = twitter.lookup_status(id=tweets_id_search, tweet_mode = "extended")
    consultas+=1 
    print("numero de consultas: "+ str(consultas))
    #Limpiar tweets
    for tweet in tweets:

        tweet_aux = {}
        user_aux_1 = {}
        user_aux = {}
        id_tweet = tweet["id"]
        hilo = csv_aux.loc[csv_aux['id'] == id_tweet]["thread_number"].tolist()[0]
        respuestas = csv_aux.loc[csv_aux['id'] == id_tweet]["replies"].tolist()[0]
        
        #limpiar dict user
        user_aux = tweet["user"]
        user_aux_1["id"] = user_aux["id"]
        user_aux_1["id_str"] = user_aux["id_str"]
        user_aux_1["name"] = user_aux["name"]
        user_aux_1["followers_count"] = user_aux["followers_count"]
        user_aux_1["friends_count"] = user_aux["friends_count"]
        user_aux_1["created_at"] = user_aux["created_at"]
        user_aux_1["verified"] = user_aux["verified"]
        user_aux_1["statuses_count"] = user_aux["statuses_count"]
        
        #crear tweet con solo datos utiles
        tweet_aux["hilo"] = hilo
        tweet_aux["created_at"] = tweet["created_at"]
        tweet_aux["id"] = tweet["id"]
        tweet_aux["id_str"] = tweet["id_str"]
        tweet_aux["text"] = tweet["full_text"]
        tweet_aux["user"] = user_aux_1
        tweet_aux["retweet_count"] = tweet["retweet_count"]
        tweet_aux["favorite_count"] = tweet["favorite_count"]
        tweet_aux["replies"] = respuestas
        tweet_aux["csv"] = csv_origen
        #hashtags
        hashtags = []
        hash_aux = []
        hashtags = tweet["entities"]["hashtags"]
        for hashtag in hashtags:
            hash_aux.append(hashtag["text"])
        tweet_aux["hashtags"] = hash_aux

        tweets_limpios.append(tweet_aux)
    #meter tweets limpios
    if len(tweets_limpios)>1:
        x = bd_prueba.insert_many(tweets_limpios)
        

#csv2
print("inicio csv2")
csv_origen = "csv2"
csv_aux = csv2
bd_prueba = mydb["csv2"]

lista_aux = []
lista_aux = csv_aux["id"].tolist()
id_all_tweets = []
id_all_tweets = cortar_lista(lista_aux, 100)

for tweets_id_search in id_all_tweets:
    #Comprobar si se pueden hacer consultas
    tweets = {}
    tweets_limpios = []
    while limite_ocupado == 1:
        consulta_estado = twitter.get_application_rate_limit_status(resources = "statuses")
        estado = consulta_estado["resources"]
        estatuses = estado["statuses"]
        estado_lookup = estatuses["/statuses/lookup"]
        sobrantes = estado_lookup["remaining"]
        if sobrantes < 1:
            time.sleep(60)
            print("esperando un minuto")
        else:
            limite_ocupado = 0
    #Conseguir tweets de la iteracion
    tweets = twitter.lookup_status(id=tweets_id_search, tweet_mode = "extended")
    consultas+=1 
    print("numero de consultas: "+ str(consultas))

    #Limpiar tweets
    for tweet in tweets:

        tweet_aux = {}
        user_aux_1 = {}
        user_aux = {}
        id_tweet = tweet["id"]
        hilo = csv_aux.loc[csv_aux['id'] == id_tweet]["thread_number"].tolist()[0]
        respuestas = csv_aux.loc[csv_aux['id'] == id_tweet]["replies"].tolist()[0]
        
        #limpiar dict user
        user_aux = tweet["user"]
        user_aux_1["id"] = user_aux["id"]
        user_aux_1["id_str"] = user_aux["id_str"]
        user_aux_1["name"] = user_aux["name"]
        user_aux_1["followers_count"] = user_aux["followers_count"]
        user_aux_1["friends_count"] = user_aux["friends_count"]
        user_aux_1["created_at"] = user_aux["created_at"]
        user_aux_1["verified"] = user_aux["verified"]
        user_aux_1["statuses_count"] = user_aux["statuses_count"]
        
        #crear tweet con solo datos utiles
        tweet_aux["hilo"] = hilo
        tweet_aux["created_at"] = tweet["created_at"]
        tweet_aux["id"] = tweet["id"]
        tweet_aux["id_str"] = tweet["id_str"]
        tweet_aux["text"] = tweet["full_text"]
        tweet_aux["user"] = user_aux_1
        tweet_aux["retweet_count"] = tweet["retweet_count"]
        tweet_aux["favorite_count"] = tweet["favorite_count"]
        tweet_aux["replies"] = respuestas
        tweet_aux["csv"] = csv_origen
        #hashtags
        hashtags = []
        hash_aux = []
        hashtags = tweet["entities"]["hashtags"]
        for hashtag in hashtags:
            hash_aux.append(hashtag["text"])
        tweet_aux["hashtags"] = hash_aux

        tweets_limpios.append(tweet_aux)
    #meter tweets limpios
    if len(tweets_limpios)>1:
        x = bd_prueba.insert_many(tweets_limpios)
        

#csv3
print("inicio csv3")
csv_origen = "csv3"
csv_aux = csv3
bd_prueba = mydb["csv3"]

lista_aux = []
lista_aux = csv_aux["id"].tolist()
id_all_tweets = []
id_all_tweets = cortar_lista(lista_aux, 100)


for tweets_id_search in id_all_tweets:
    #Comprobar si se pueden hacer consultas
    tweets = {}
    tweets_limpios = []
    limite_ocupado = 1
    while limite_ocupado == 1:
        consulta_estado = twitter.get_application_rate_limit_status(resources = "statuses")
        estado = consulta_estado["resources"]
        estatuses = estado["statuses"]
        estado_lookup = estatuses["/statuses/lookup"]
        sobrantes = estado_lookup["remaining"]
        if sobrantes < 1:
            time.sleep(60)
            print("esperando un minuto")
        else:
            limite_ocupado = 0
    #Conseguir tweets de la iteracion
    tweets = twitter.lookup_status(id=tweets_id_search, tweet_mode = "extended")
    consultas+=1 
    print("numero de consultas: "+ str(consultas))
    #Limpiar tweets
    for tweet in tweets:

        tweet_aux = {}
        user_aux_1 = {}
        user_aux = {}
        id_tweet = tweet["id"]
        hilo = csv_aux.loc[csv_aux['id'] == id_tweet]["thread_number"].tolist()[0]
        respuestas = csv_aux.loc[csv_aux['id'] == id_tweet]["replies"].tolist()[0]
        
        #limpiar dict user
        user_aux = tweet["user"]
        user_aux_1["id"] = user_aux["id"]
        user_aux_1["id_str"] = user_aux["id_str"]
        user_aux_1["name"] = user_aux["name"]
        user_aux_1["followers_count"] = user_aux["followers_count"]
        user_aux_1["friends_count"] = user_aux["friends_count"]
        user_aux_1["created_at"] = user_aux["created_at"]
        user_aux_1["verified"] = user_aux["verified"]
        user_aux_1["statuses_count"] = user_aux["statuses_count"]
        
        #crear tweet con solo datos utiles
        tweet_aux["hilo"] = hilo
        tweet_aux["created_at"] = tweet["created_at"]
        tweet_aux["id"] = tweet["id"]
        tweet_aux["id_str"] = tweet["id_str"]
        tweet_aux["text"] = tweet["full_text"]
        tweet_aux["user"] = user_aux_1
        tweet_aux["retweet_count"] = tweet["retweet_count"]
        tweet_aux["favorite_count"] = tweet["favorite_count"]
        tweet_aux["replies"] = respuestas
        tweet_aux["csv"] = csv_origen
        #hashtags
        hashtags = []
        hash_aux = []
        hashtags = tweet["entities"]["hashtags"]
        for hashtag in hashtags:
            hash_aux.append(hashtag["text"])
        tweet_aux["hashtags"] = hash_aux

        tweets_limpios.append(tweet_aux)
    #meter tweets limpios
    if len(tweets_limpios)>1:
        x = bd_prueba.insert_many(tweets_limpios)
        

#csv4
print("inicio csv4")
csv_origen = "csv4"
csv_aux = csv4
bd_prueba = mydb["csv4"]

lista_aux = []
lista_aux = csv_aux["id"].tolist()
id_all_tweets = []
id_all_tweets = cortar_lista(lista_aux, 100)

for tweets_id_search in id_all_tweets:
    #Comprobar si se pueden hacer consultas
    tweets = {}
    tweets_limpios = []
    limite_ocupado = 1
    while limite_ocupado == 1:
        consulta_estado = twitter.get_application_rate_limit_status(resources = "statuses")
        estado = consulta_estado["resources"]
        estatuses = estado["statuses"]
        estado_lookup = estatuses["/statuses/lookup"]
        sobrantes = estado_lookup["remaining"]
        if sobrantes < 1:
            time.sleep(60)
            print("esperando un minuto")
        else:
            limite_ocupado = 0
    #Conseguir tweets de la iteracion
    tweets = twitter.lookup_status(id=tweets_id_search, tweet_mode = "extended")
    consultas+=1 
    print("numero de consultas: "+ str(consultas))

    #Limpiar tweets
    for tweet in tweets:

        tweet_aux = {}
        user_aux_1 = {}
        user_aux = {}
        id_tweet = tweet["id"]
        hilo = csv_aux.loc[csv_aux['id'] == id_tweet]["thread_number"].tolist()[0]
        respuestas = csv_aux.loc[csv_aux['id'] == id_tweet]["replies"].tolist()[0]
        
        #limpiar dict user
        user_aux = tweet["user"]
        user_aux_1["id"] = user_aux["id"]
        user_aux_1["id_str"] = user_aux["id_str"]
        user_aux_1["name"] = user_aux["name"]
        user_aux_1["followers_count"] = user_aux["followers_count"]
        user_aux_1["friends_count"] = user_aux["friends_count"]
        user_aux_1["created_at"] = user_aux["created_at"]
        user_aux_1["verified"] = user_aux["verified"]
        user_aux_1["statuses_count"] = user_aux["statuses_count"]
        
        #crear tweet con solo datos utiles
        tweet_aux["hilo"] = hilo
        tweet_aux["created_at"] = tweet["created_at"]
        tweet_aux["id"] = tweet["id"]
        tweet_aux["id_str"] = tweet["id_str"]
        tweet_aux["text"] = tweet["full_text"]
        tweet_aux["user"] = user_aux_1
        tweet_aux["retweet_count"] = tweet["retweet_count"]
        tweet_aux["favorite_count"] = tweet["favorite_count"]
        tweet_aux["replies"] = respuestas
        tweet_aux["csv"] = csv_origen
        #hashtags
        hashtags = []
        hash_aux = []
        hashtags = tweet["entities"]["hashtags"]
        for hashtag in hashtags:
            hash_aux.append(hashtag["text"])
        tweet_aux["hashtags"] = hash_aux

        tweets_limpios.append(tweet_aux)
    #meter tweets limpios
    if len(tweets_limpios)>1:
        x = bd_prueba.insert_many(tweets_limpios)
        

#csv5
print("inicio csv 5")
csv_origen = "csv5"
csv_aux = csv5
bd_prueba = mydb["csv5"]

lista_aux = []
lista_aux = csv_aux["id"].tolist()
id_all_tweets = []
id_all_tweets = cortar_lista(lista_aux, 100)

for tweets_id_search in id_all_tweets:
    #Comprobar si se pueden hacer consultas
    tweets = {}
    tweets_limpios = []
    limite_ocupado = 1
    while limite_ocupado == 1:
        consulta_estado = twitter.get_application_rate_limit_status(resources = "statuses")
        estado = consulta_estado["resources"]
        estatuses = estado["statuses"]
        estado_lookup = estatuses["/statuses/lookup"]
        sobrantes = estado_lookup["remaining"]
        if sobrantes < 1:
            time.sleep(60)
            print("esperando un minuto")
        else:
            limite_ocupado = 0
    #Conseguir tweets de la iteracion
    tweets = twitter.lookup_status(id=tweets_id_search, tweet_mode = "extended")
    consultas+=1 
    print("numero de consultas: "+ str(consultas))

    #Limpiar tweets
    for tweet in tweets:

        tweet_aux = {}
        user_aux_1 = {}
        user_aux = {}
        id_tweet = tweet["id"]
        hilo = csv_aux.loc[csv_aux['id'] == id_tweet]["thread_number"].tolist()[0]
        respuestas = csv_aux.loc[csv_aux['id'] == id_tweet]["replies"].tolist()[0]
        
        #limpiar dict user
        user_aux = tweet["user"]
        user_aux_1["id"] = user_aux["id"]
        user_aux_1["id_str"] = user_aux["id_str"]
        user_aux_1["name"] = user_aux["name"]
        user_aux_1["followers_count"] = user_aux["followers_count"]
        user_aux_1["friends_count"] = user_aux["friends_count"]
        user_aux_1["created_at"] = user_aux["created_at"]
        user_aux_1["verified"] = user_aux["verified"]
        user_aux_1["statuses_count"] = user_aux["statuses_count"]
        
        #crear tweet con solo datos utiles
        tweet_aux["hilo"] = hilo
        tweet_aux["created_at"] = tweet["created_at"]
        tweet_aux["id"] = tweet["id"]
        tweet_aux["id_str"] = tweet["id_str"]
        tweet_aux["text"] = tweet["full_text"]
        tweet_aux["user"] = user_aux_1
        tweet_aux["retweet_count"] = tweet["retweet_count"]
        tweet_aux["favorite_count"] = tweet["favorite_count"]
        tweet_aux["replies"] = respuestas
        tweet_aux["csv"] = csv_origen
        #hashtags
        hashtags = []
        hash_aux = []
        hashtags = tweet["entities"]["hashtags"]
        for hashtag in hashtags:
            hash_aux.append(hashtag["text"])
        tweet_aux["hashtags"] = hash_aux

        tweets_limpios.append(tweet_aux)
    #meter tweets limpios
    if len(tweets_limpios)>1:
        x = bd_prueba.insert_many(tweets_limpios)
        
