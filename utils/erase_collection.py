
import pymongo
import os 
import shutil

def erase():
	myclient = pymongo.MongoClient("mongodb://localhost:27017/") #conexion con el gestor de base de datos

	db = myclient["mydatabase"]

	mycollection= db["uri_state"]

	mycollection.drop()

	myclient.close()

erase()
bdir = os.getcwd()
shutil.rmtree(bdir +  "/audio")
os.mkdir(bdir +  "/audio")
