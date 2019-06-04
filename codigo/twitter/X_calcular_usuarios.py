import numpy as np
import matplotlib
import pandas as pd

import pymongo
from twython import Twython
import pandas as pd
#conexion mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]
coleccion_completa = mydb["csv_all"]
bd_usuarios = mydb["usuarios"]


#obtener llaves Twitter
llaves_bd = mydb["llaves"]
llaves = llaves_bd.find_one()

APP_KEY = llaves["API_KEY"]
ACCESS_TOKEN  = llaves["ACCESS_TOKEN_T"]
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)


usuarios = bd_usuarios.find({})

for usuario in usuarios:
    puntaje_aux = 0
    puntaje_aux_2 = 0
    userid = usuario["id"]
    seguidores = usuario["followers_count"] + 1
    amigos = usuario["friends_count"] + 1
    estados = usuario["statuses_count"] + 1
    #puntaje_aux = ((seguidores-amigos)/(seguidores+amigos-abs(seguidores-amigos)))
    puntaje_aux = (np.log10(seguidores))/(1+np.log10(amigos))

    myquery = { "id": userid }
    newvalues = { "$set": { "puntaje_sigmo": puntaje_aux } }
    agregar_amigos = bd_usuarios.update_one(myquery, newvalues)    
    
