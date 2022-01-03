import logging
import pymongo
import json
import time
import pyrogram
import multiprocessing
import os
from pyrogram.errors import FloodWait
import datetime
import shutil
from pathlib import Path


def start_process(target, args):
	process = multiprocessing.Process(target = target, args = args)
	process.start()

def set_session_state(config, client, session: int, state : int):
	session_collection = client[config['db_name']]['sesion_state']
	field_to_update = {"$set" : {"state": state}}
	session_collection.update_one( {"session" : session } , field_to_update)

def pending_uri(
	collection: pymongo.collection.Collection
	):
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


def ask_for_media_and_download(
	client: pyrogram.Client,
	config: dict,
	session_string: str ,
	 uri: str,
	 collection: pymongo.collection.Collection,
	 session_selected):

	#BOT CHAT NAME 
	chat_name = "spotify_down_bot"

	#TO SEE SESSION OWNER
	logging.info(f'First_name {client.get_me().first_name}')
	
	#CLEAN CHAT
	messages = client.iter_history(chat_name, reverse = True)

	for index,message in enumerate(messages):
		if index > 0:
			client.delete_messages(chat_name,message.message_id)

	#SENDING URI
	sms = "/download spotify:track:" + uri
	client.send_message(chat_name, sms)	
	
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
				return False

		if len(messages) == 4 :
			#the media aparece en el sms 2 o el 3
			index = 2 if messages[2]['audio'] else 3
			if messages[index]['audio']:
				logging.info(f'Media available. Title: {messages[index]["audio"]["title"]}')
				no_media = False
			else:
				logging.error(f'Uri is not in Deezer database, it cannot be downloaded.')
				update_database(collection, uri, state = "ERROR", path = f'This uri {uri} is not in Deezer database, it cannot be downloaded.')
				return False

		end = time.time()
		#MINIMUN TIME TO FIND SONG 
		'''
			We can't permanently get message history cause an error raise up
		'''
		if end - start > config["download_wait_time"]:
			logging.error(f'Error. Timeout waiting available download for {uri}')
			update_database(collection, uri, state = "ERROR", path = f'Timeout for waiting available download using session {session_selected}')
			return False

	#DOWNLOAD SINGLE MEDIA
	for i in range(3):
		try:
			time.sleep(2)
			download_path = client.download_media(messages[index]['audio'])
			
			## To test without download
			# time.sleep(3)
			# download_path = "Some test path"
			##

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
			return False
		
		except:
			logging.info(f'Attemp {i+1} to download media')
			if i + 1 == 3:
				logging.info("Media wasn't downloaded")
				download_path = None

	if download_path:
		shutil.move(download_path,f'audio/1{Path(download_path).name}')
		final_path = os.getcwd() + f'/audio/1{Path(download_path).name}'
		

		logging.info("Media available at : " + final_path)

		## To test wituout download media
		#final_path = download_path
		##

		update_database(collection, uri, "OK", final_path)
		print(f'{uri} download by {session_selected}')
		return True

	else:
		update_database(collection, uri, state = "PENDING", path ="Error occur in media download ")
		logging.error("Error during media download.")
		return False


def increase_download_number(collection,session_selected):
	collection.update_one({"session" : session_selected}, { "$inc" : {"downloads": 1 }})
	downloads = collection.find_one({"session": session_selected},{"downloads": 1})
	downloads = downloads['downloads']
	logging.warning(f'Number of downloads by session {session_selected}: {downloads}')
	

def session_dowloader(session, config):
	#STARTING MONGO CLIENT
	mgClient = pymongo.MongoClient(config['db_conection'])
	#EXTARCT COLLECTIONS
	uri_collection = mgClient[config['db_name']][config['collection_name']]
	session_collection = mgClient[config['db_name']]['sesion_state']

	#STARTING TELEGRAM CLIENT BY STRING SESSION
	with open(f'sessions/session{session}.txt') as sfile:
		session_string = sfile.read()
	tgClient = pyrogram.Client(session_string, config["api_id"], config["api_hash"]) 
	tgClient.start()

	#GET PENDING URI
	while True:
		uri = pending_uri(uri_collection)
		if uri:
			try:
				media_download = ask_for_media_and_download(tgClient,config, session_string, uri,uri_collection,session)
			except FloodWait as e:
				logging.error(f'FloodWait ocurr with the session {session} and uri {uri}.\n {uri} set to PENDING')
				update_database(collection = uri_collection, uri = uri, state = "PENDING" , path="PENDING")
				set_session_state(config, mgClient ,session, 2)
				current_date = datetime.datetime.now()
				with open("floodTime.txt","a") as ftfile:
					ftfile.write(f'Flood time {e.x} seconds by session {session} at {current_date}\n')
				time.sleep(e.x)

			if media_download:
				increase_download_number(session_collection,session)


def singleDownload():
	logging.basicConfig(level = logging.INFO)

	with open("config.json","r") as config_file:
		config = json.load(config_file)

	client = pymongo.MongoClient(config['db_conection'])

	# set_session_state(config,client,session = 1,state = 0)
	# set_session_state(config,client,session = 2,state = 0)
	# set_session_state(config,client,session = 3,state = 0)
	set_session_state(config,client,session = 4,state = 0)
	# set_session_state(config,client,session = 5,state = 0)
	# set_session_state(config,client,session = 6,state = 0)
	# set_session_state(config,client,session = 7,state = 0)
	# set_session_state(config,client,session = 8,state = 0)

	#LOOP LOCKING FOR FREE SESSION ALL TIME
	while True:
		session_collection = client[config['db_name']]['sesion_state']
		free_sessions_coll = session_collection.find({"state" : 0},{"session":1})
		free_sessions = [item['session'] for item in free_sessions_coll]
		
		if free_sessions:
			print(f'Free sessions {free_sessions}')

			#START A DOWNLOAD PROCESS FOR EACH FREE SESSION
			for session in free_sessions:
				set_session_state(config,client,session,1)
				print(f'{session} set to 1')
				start_process(target = session_dowloader, args =[session,config] )
				time.sleep(2) # to avoid start with the same uri


if __name__ == "__main__":
	singleDownload()

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
	*Probar en el servidor para una sola session X
		*Agregar campo downloads a session X
		*Agregar llaves a config.json X
	*Refactorizar para que cada session se inicie solo una vez X
	*Instalar ffmepg en server
	*Instalar pydub
	*Instalr dropbox

	

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