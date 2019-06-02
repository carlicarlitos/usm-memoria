import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)


coleccion = mydb["csv1"]

lista = ["hola"]

for item in lista:
    print(item)
"""
tweet_id = 971825194909265924
tweet = twitter.lookup_status(id=tweet_id, tweet_mode = "extended")
hashtags = tweet[0]["entities"]["hashtags"]
for hashtag in hashtags:
    print(hashtag["text"])

x;
"""