import pymongo
import random



def fill():
  myclient = pymongo.MongoClient("mongodb://localhost:27017/") #conexion con el gestor de base de datos

  db = myclient["mydatabase"]

  mycollection= db["uri_state"]

  lista = []

  with open("cUri.txt", "r")  as uri_file:
    for uri in uri_file.readlines():
      priority = random.randint(0,3)
      lista.append({"uri": uri[0:-1], "state": "PENDING", "path": "PENDING" , "priority": priority })
  

  x = mycollection.insert_many(lista)
  x = mycollection.insert_one({"uri": "4gOMf7ak5Ycx9BghTCSTBL", "state": "PENDING", "path": "PENDING" , "priority": 0 })
  print(myclient.list_database_names())
  print(db.list_collection_names())
  for x in mycollection.find():
    print(x)
  myclient.close()

fill()
