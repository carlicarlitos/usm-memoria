import pymongo
import os
import random
from twython import Twython
import pandas as pd
import time
import dicttoxml



THIS_FOLDER = os.getcwd()

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

carpeta_guardar = "documentos"
camino = os.path.join(THIS_FOLDER, carpeta_guardar)
titulo = "\\hashtags.basket"
nombre_archivo = camino + titulo

file_hashs = open(nombre_archivo, "w+", encoding = "utf-8")


for hilo in hilos:
    set_hashs = set()
    lista = []
    hasht = hilo["all_hashtags"]
    hashtags = list(x.lower() for x in hasht)
    print(hashtags)

#lista de hashtags por tema
    trump_drama = ["trump","russia","russian","trumprussia", "impeach", "trumpdossier","qanon","qanons"]
    tecnologia = ["cambridgeanalytica","cambridgeanalytics","bitcoin","ai","facebook","brexit", "womeninstem"]
    farandula = ["royalwedding","metoo", "oscars"]
    elecciones = ["bluewave2018","vote", "midterms2018","illinois", "georgia"]
    escandalo = ["fakenews", "cultureofcorruption", "unhackthevote", "draintheswamp", "theresistance"]
    eeuu_politica = ["nra","dreamers","steeledossier", "grassleymemo","releasethememo", "memoday","america","american", "americans", "obama","obamagate","clintoncampaign","crookedhillary", "clintonfoundation", "billclinton"]
    salud = ["health", "medicaid", "ayurveda"]
#etiquetado de hilos
    if (len(hashtags) == 0):
        texto = "nohashtags"
#farandula    
    elif any(c in hashtags for c in farandula):
        texto = "farandula_internacional"
#tecnologia 
    elif any(c in hashtags for c in tecnologia):
        texto = "tecnologia"
#elecciones
    elif any(c in hashtags for c in elecciones):
        texto = "elecciones_eeuu"
#salud
    elif any(c in hashtags for c in salud):
        texto = "salud"        
#american_scandal
    elif any(c in hashtags for c in escandalo):
        texto = "american_scandal"
#politica gringa
    elif any(c in hashtags for c in eeuu_politica):
        texto = "american_politics"
# american laws and drama
    elif any(c in hashtags for c in trump_drama):
        texto = "trumprussia"
    
    else: 
        texto = "otros"

    file_hashs.write(texto)
    file_hashs.write("\n")

file_hashs.close()
"""
all_hashs = set()
for hilo in hilos:
    hashtags = hilo["all_hashtags"]
    all_hashs.update(hashtags)

lista_hashtags = list(all_hashs)
cant_hashs = len(lista_hashtags)

cabeza = ",".join(lista_hashtags)
file_hashs.write(cabeza)
file_hashs.write("\n")
fila_cero = [0]*cant_hashs

hilos = bd_hilos.find({})

for hilo in hilos:
    fila = [0]*cant_hashs
    hashtags = hilo["all_hashtags"]
    if len(hashtags) > 0:
        for hashtag in hashtags:
            posi = lista_hashtags.index(hashtag)
            fila[posi] = 1
    if fila != fila_cero:
        texto = ",".join(str(x) for x in fila)
        file_hashs.write(texto)
        file_hashs.write("\n")
"""
file_hashs.close()