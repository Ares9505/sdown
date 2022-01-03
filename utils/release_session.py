import pymongo
import os 
import shutil
import json

with open("config.json","r") as config_file:
	config = json.load(config_file)

client = pymongo.MongoClient("mongodb://localhost:27017/") #conexion con el gestor de base de dato
session_collection = client[config['db_name']]['sesion_state']
field_to_update = {"$set" : {"state": 0}}
session_collection.update_one( {"session" : 1 } , field_to_update)