import json
import pymongo
from twython import Twython


#Configs
with open('keys.json') as f:
    d = json.load(f)

APP_KEY = d["API_KEY"]
APP_SECRET = d["API_SECRET_KEY"]

twitter       = Twython(APP_KEY, APP_SECRET, oauth_version=2)
ACCESS_TOKEN  = twitter.obtain_access_token()
twitter       = Twython(APP_KEY, access_token=ACCESS_TOKEN)

#Mongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["twitter-memoria"]

mycol = mydb["llaves"]

d["ACCESS_TOKEN_T"] = ACCESS_TOKEN
clave = {"ACCESS_TOKEN": ACCESS_TOKEN}

x = mycol.insert_one(d)
x = mycol.insert_one(clave)

