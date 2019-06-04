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
#conseguir amigos o seguidos
for usuario in dict_usuarios:
    user_id = usuario["id"]
    print("usuario: " + str(user_id))
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
                print("esperando un minuto")
                time.sleep(60)
            else:
                limite_ocupado = 0
        
        #conseguir lista amigos
        print("consultando amigos")
        consulta_amigos = twitter.get_friends_ids(user_id = user_id, cursor = siguiente)
        amigos.append(consulta_amigos["ids"])
        siguiente = consulta_amigos["next_cursor"]
    amigos_lista = [item for sublist in amigos for item in sublist]

    #crear relacion de amistad    
    for amigo in amigos_lista:
        n_user = "n"+str(amigo)
        origen = "Conseguido"

        #Consultar si existe nodo a agregar        
        consulta_nodo = f"""MATCH (n) WHERE (n.id = {amigo}) RETURN n"""
        result = gdb.query(consulta_nodo, data_contents=True)
        nodo_obtenido = result.rows

        #Si no existe amigo, crear
        if nodo_obtenido is None:
            crear_nodo = f"""CREATE ({n_user}:User {{ id : {amigo}, origen: '{origen}' }})"""
            result = gdb.query(crear_nodo, data_contents=True)        

        #consultar amistad: user_id sigue a amigo.
        consultar_amistad = f"""RETURN EXISTS( (:User {{ id: {user_id} }})-[:Follows]->(:User {{ id: {amigo} }} ) )"""
        result = gdb.query(consultar_amistad, data_contents=True)
        lo_sigo = result.rows[0][0]
        
        #crear amistad si no existe
        if lo_sigo is False:
            crear_amistad = f"""MATCH (yo),(amigo) WHERE (yo.id = {user_id}) AND (amigo.id = {amigo}) CREATE (yo) -[r:Follows] -> (amigo)"""
            result = gdb.query(crear_amistad, data_contents=True)

