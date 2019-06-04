from neo4jrestclient.client import GraphDatabase
import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time


#conecion neo4j
gdb = GraphDatabase("http://localhost:7474/db/data/", username="neo4j", password="memoria")

#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]

#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

db_usuarios = mydb["usuarios"]

dict_usuarios = db_usuarios.find({})

n_user = ""
for usuario in dict_usuarios:
    user_id = usuario["id"]
    n_user = "n"+str(user_id)
    origen = "original"
    crear_nodo = f"""CREATE ({n_user}:User {{ id : {user_id}, origen: '{origen}' }})"""
    result = gdb.query(crear_nodo, data_contents=True)
