import pyrogram
import json
import time
import asyncio
import logging
import shutil
from pathlib import Path
import random
import os
import pymongo
import multiprocessing
from pyrogram.errors import FloodWait
import datetime

'''
	Esta rama require flood avoid time: 60
	y download limit: 120 en config.json
'''

def check_free_sessions(config,client):
	'''
	Query sesion_state collection from sdown database for free sessions
	'''
	while True:
		session_collection = client[config['db_name']]['sesion_state']
		free_sessions_coll = session_collection.find({"state" : 0},{"session":1})
		free_sessions = [item['session'] for item in free_sessions_coll]	

		if free_sessions:
			return free_sessions

		
def ask_for_media_and_download(
	config: dict,
	session_string: str ,
	 uri: str,
	 collection: pymongo.collection.Collection,
	 session_selected):	
	'''
		*Start telegram client for a given session, show the owner name
		*Delete old messages except the star bot message
		*Send message with given uri
		*wait for media available 
			*Ask 'every get_history_time' for chat history
			*If response bot message is uri no valid set error state in uri
			*If response bot message is not in Dazzer set error state in uri
			*If exceed 'download_time_wait' set error state in uri 
	'''

	#BOT CHAT NAME 
	chat_name = "spotify_down_bot" # access only via web

	#CONNECT CLIENT
	client = pyrogram.Client(session_string, config["api_id"], config["api_hash"]) 
	client.start()

	#TO SEE SESSION OWNER
	logging.info(f'First_name {client.get_me().first_name}')
	
	#CLEAN CHAT
	messages = client.iter_history(chat_name, reverse = True)

	for index,message in enumerate(messages):
		if index > 0:
			client.delete_messages(chat_name,message.message_id)

	# #SENDING URI
	sms = "/download spotify:track:" + uri
	client.send_message(chat_name, sms)	

	
	#WAIT FOR MEDIA AVAILABLE
	no_media = True
	start = time.time()
	while(no_media):
		logging.info("Waiting for download available")
		time.sleep(config["get_history_time"])
		messages = client.iter_history(chat_name, reverse = True)

		for index,message in enumerate(messages):
			print(index, message.text)

		if len(messages) == 3:
			text = messages[2].text 
			if "🚫" in text:
				logging.error(f'Error. {uri} no valido')
				update_database(collection, uri, state = "ERROR", path = "Uri no valido")
				return

		if len(messages) == 4 :
			#the media aparece en el sms 2 o el 3
			index = 2 if messages[2]['audio'] else 3
			if messages[index]['audio']:
				logging.info(f'Media available. Title: {messages[index]["audio"]["title"]}')
				no_media = False
			else:
				logging.error(f'Uri is not in Deezer database, it cannot be downloaded.')
				update_database(collection, uri, state = "ERROR", path = f'This uri {uri} is not in Deezer database, it cannot be downloaded.')
				return

		end = time.time()
		#MINIMUN TIME TO FIND SONG 
		'''
			We can't permanently get message history cause an error raise up
		'''
		if end - start > config["download_wait_time"]:
			logging.error(f'Error. Timeout waiting available download for {uri}')
			update_database(collection, uri, state = "ERROR", path = f'Timeout for waiting available download using session {session_selected}')
			return

	#DOWNLOAD SINGLE MEDIA
	for i in range(3):
		try:
			time.sleep(2)
			download_path = client.download_media(messages[index]['audio'])
			
			# ## To test without download
			# time.sleep(3)
			# download_path = "Some test path"
			# ##

			logging.info("Downloading media")
			break

		except FloodWait as e:
			logging.error(f'FloodWait ocurr with the session {session_selected} and uri {uri}.\n {uri} set to PENDING')
			update_database(collection = collection, uri = uri, state = "PENDING" , path="PENDING")
			set_session_state(config, client ,session_selected, 2)
			current_date = datetime.datetime.now()
			with open("floodTime.txt","a") as ftfile:
				ftfile.write(f'Flood time {e.x} seconds by session {session_selected} at {current_date}\n')
			time.sleep(e.x)
		
		except:
			logging.info(f'Attemp {i+1} to download media')
			if i + 1 == 3:
				logging.info("Media wasn't downloaded")
				download_path = None

	if download_path:
		shutil.move(download_path,f'audio/1{Path(download_path).name}')
		final_path = os.getcwd() + f'/audio/1{Path(download_path).name}'
		logging.info("Media available at : " + final_path)

		# ## To test wituout download
		# final_path = download_path
		# ##

		update_database(collection, uri, "OK", final_path)
		print(f'{uri} by {session_selected}')
		return

	else:
		update_database(collection, uri, state = "ERROR", path ="Error occur in media download ")
		logging.error("Error during media download.")
		return


	return


def set_session_state(config, client, session: int, state : int):

	session_collection = client[config['db_name']]['sesion_state']
	field_to_update = {"$set" : {"state": state}}
	session_collection.update_one( {"session" : session } , field_to_update)


def pending_uri(
	collection: pymongo.collection.Collection
	) -> dict:
	'''
		*Extract doc with mayor priority and state equal pending 
		*Update doc extracted with state =  Procesing
	'''	
	logging.info("Loocking your pending uri...")
	while True:
		uri_doc = collection.find_one({"state": "PENDING"},sort = [("priority",-1)])

		if uri_doc:
			uri = uri_doc["uri"]
			uri_doc_id = {"uri" : uri}
			field_to_update = {"$set" : {"state": "PROCESING"}}
			collection.update_one( uri_doc_id, field_to_update)
			logging.info(f'Uri {uri} is being processed')
			return uri		
	


def update_database(
	collection: pymongo.collection.Collection,
	uri: str,
	state: str,
	path: str ):

	field_to_update = {"$set" : {"state": state, "path": path}}
	collection.update_one( {"uri" : uri } , field_to_update)


def start_process(target, args):
	process = multiprocessing.Process(target = target, args = args)
	process.start()


def download_task(
	config: dict,  
	session_selected: int,
	uri: str
	):
	
	client = pymongo.MongoClient(config['db_conection'])
	collection = client[config['db_name']][config['collection_name']]
	with open(f'sessions/session{session_selected}.txt') as sfile:
		session_string_selected = sfile.read()
	

	#FLOODWAIT ERROR HAPPEND CAUSE A REPEATED GET HISTORY ACTION IN ask_for_media_download FUNCTION
		#Logs for flood time ocurrence apear in FloodTime.txt 
	try:
		ask_for_media_and_download(config, session_string_selected, uri,collection,session_selected)

	except FloodWait as e:
		logging.error(f'FloodWait ocurr with the session {session_selected} and uri {uri}.\n {uri} set to PENDING')
		update_database(collection = collection, uri = uri, state = "PENDING" , path="PENDING")
		set_session_state(config, client ,session_selected, 2)
		current_date = datetime.datetime.now()
		with open("floodTime.txt","a") as ftfile:
			ftfile.write(f'Flood time {e.x} seconds by session {session_selected} at {current_date}\n')
		time.sleep(e.x)
		return

	#CHECK DOWNLOAD REACH LIMIT FOR SESSION:
	#check_download_limit_by_session(config = config, session_selected = session_selected, client = client)

	#AWAIT RANDOM TIME
	logging.info(f'Session {session_selected} awaiting 40 - 60 seconds after download')
	await_random_time()

	#SET SESSION READY
	set_session_state(config, client ,session_selected, 0)

def await_random_time():
	stime = random.randint(40,60)
	time.sleep(stime)

def check_download_limit_by_session(config, session_selected, client):
	'''
		*From session_state collection increase the downloads numbers of a particular session
		*If the download is higher the download limit by session the session operation will be paused a flood_avoid_time
	'''
	collection = client[config['db_name']]['sesion_state']
	collection.update_one({"session" : session_selected}, { "$inc" : {"downloads": 1 }})
	downloads = collection.find_one({"session": session_selected},{"downloads": 1})
	downloads = downloads['downloads']
	logging.warning(f'Number of downloads by session {session_selected}: {downloads}')
	
	if downloads['downloads'] > config['downloads_limit']:
		collection.update_one({"session" : session_selected}, { "$set" : {"state": 3, "downloads": 0 }})#wait state
		logging.warning(f'Session {session_selected} stop for avoid Flood time')
		time.sleep(config['flood_avoid_time'])
	return	

'''
	Modificar:
	*Annadir en sesion_state el campo downloads para cada session X
	*Annadir download_wait y flood_avoid_time a config.json X
'''

def test():
	logging.basicConfig(level = logging.INFO )
	logger = logging.getLogger("singleDownload")

	with open("config.json","r") as config_file:
		config = json.load(config_file)


	logger.info("Connecting to MongoDB...")

	client = pymongo.MongoClient(config['db_conection'])
	for i in range(0,100):
		check_download_limit_by_session(config = config , session_selected = 1, client = client)

#MAIN
def singleDownload():
	logging.basicConfig(level = logging.INFO )
	logger = logging.getLogger("singleDownload")

	with open("config.json","r") as config_file:
		config = json.load(config_file)


	logger.info("Connecting to MongoDB...")

	client = pymongo.MongoClient(config['db_conection'])

	collection = client[config['db_name']][config['collection_name']]			
	cant = 0
	condition =True	

	while condition:
		#check_free_session wait for a free session in a loop
		session_free_list = check_free_sessions(config, client)
		session_selected = random.choice(session_free_list)
		logger.info(f'Session seleccionada: {session_selected}')
		uri = pending_uri(collection)
		if uri:
				#SET SESSION BUSSY					
			set_session_state(config, client, session_selected, 1)
			print(f'Session {session_selected}, uri {uri} {time.time()}')
			start_process(target = download_task, args = [config,session_selected, uri])
			cant +=1
			print(cant)


if __name__ == "__main__":	
	#singleDownload()
	#test()
	await_random_time()

'''
Tareas:
	*Cambiar por session_string la conexion del cliente x (storage_sessions.py)
	*Agregar seteo de estados del descagador x
	*Crear y llenar base de datos de prueba  x
	*Consulta a base de datos para ver si hay uris pendientes y extraer la de mayor prioridad x
	*Descargar uri pendiente si hay alguna sesion desocupada x
	*Tener en cuenta uri con dos medias para descargar (Esto no ocurre nunca) X
	*Establecer lazo while true para que se chequee eternamente la base de  datos x
	*Hacer funcion que actualice la base de datos con los estados debidos x
	*Instalar telegra desktop para checar envio de sms x
	*Conectar con la base de datos de serer (necesario descargar compass) x
	*Llenar base de datos con path de la cancion descargada segun uri x
	*Instalar compass y unirme a base de datos de servidor X
	*agregar setear estado de base de datos x
	*Probar con las diferentes sessiones la asincronia X
		Probar en server X (No sirvio asincronia con lectura de txt se va a probar cpn mongdb)
	*Annadir descarga asincronica por session usando mongodb collection session_state X
	*Cambiar en configuracion en config.json y poner la direccion para la conexion con la base de  datos,
	el nombre de la base de datos X
	*Agregar manejo de error cuand no esta en dezzer database X
	*Agregar manejo de flood wait error X
	*Agregar control sobre canciones descargadas por session para evitar flood wait X
	*Probar en el servidor para una sola session 
		*Agregar campo downloads a session d
		*Agregar llaves a config.json
	
Pruebas realizadas:
*que se pare cada 120 canciones 60 seg X flodd wait a las 66 descargas
*que se pare cada 60 canciones 60 seg X flood wait en la 3era vuelta 154 descargas
(se incrementa el tiempo de flood wait respecto al flood wait anterior)
*Prueba aislada para ver cuantos get history se pueden hace cada 3 segundaos ()
*que espere de 10 - 40  seg para descargar entre cancion y cancion con una sola session
	
Notas:
	*Recomendacion para manejo de foolwait exception https://docs.pyrogram.org/start/errors
	
	*Se annadi flood wait a intento de descarga

Sessiones:
	1 dariel
	2 elisabeth
	3 ale
	4 sherry
	5 key
	6 naty
	7 Dayron
	8 Mama
'''

