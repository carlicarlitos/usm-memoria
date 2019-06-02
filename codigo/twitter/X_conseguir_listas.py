import pymongo
from twython import Twython
import pandas as pd
import time
import numpy as np

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
users = usuarios#[:3]
total = 0

#conseguir amigos o seguidos
for usuario in users:
    userid = usuario["id"]
    amigos = []
    siguiente = -1
    #ciclo hasta que no hayan más paginas de amigos
    while siguiente !=0:
        #Comprobar si se pueden hacer consultas
        limite_ocupado = 1
        while (limite_ocupado == 1):
            consulta_estado = twitter.get_application_rate_limit_status(resources = "friends")
            resource = consulta_estado["resources"]
            tipo_consulta = resource["friends"]
            consulta = tipo_consulta["/friends/ids"]
            sobrantes = consulta["remaining"]
            print("consultas sobrantes: " + str(sobrantes))
            if sobrantes < 1:
                time.sleep(60)
                print("esperando un minuto")
            else:
                limite_ocupado = 0
        #get seguidos

        consulta_amigos = twitter.get_friends_ids(user_id = userid, cursor = siguiente)
        amigos.append(consulta_amigos["ids"])
        siguiente = consulta_amigos["next_cursor"]
    
    myquery = { "id": userid }
    newvalues = { "$set": { "amigos": amigos } }
    agregar_amigos = bd_usuarios.update_one(myquery, newvalues)

#conseguir seguidores
for usuario in users:
    userid = usuario["id"]
    seguidores = []
    siguiente = -1
    #ciclo hasta que no hayan más paginas de seguidores
    while siguiente !=0:
        #Comprobar si se pueden hacer consultas
        limite_ocupado = 1
        while (limite_ocupado == 1):
            consulta_estado = twitter.get_application_rate_limit_status(resources = "friends")
            resource = consulta_estado["resources"]
            tipo_consulta = resource["friends"]
            consulta = tipo_consulta["/friends/following/ids"]
            sobrantes = consulta["remaining"]
            print("consultas sobrantes: " + str(sobrantes))
            if sobrantes < 1:
                time.sleep(60)
                print("esperando un minuto")
            else:
                limite_ocupado = 0
        #get seguidores

        consulta_seguidores = twitter.get_followers_ids(user_id = userid, cursor = siguiente)
        seguidores.append(consulta_seguidores["ids"])
        siguiente = consulta_seguidores["next_cursor"]
    
    myquery = { "id": userid }
    newvalues = { "$set": { "seguidores": seguidores } }
    agregar_seguidores = bd_usuarios.update_one(myquery, newvalues)
