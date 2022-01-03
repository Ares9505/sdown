import pyrogram
import json
import jsonpickle

def storage_session_data(old_session : bool = False):
	'''
		Function to save session information (API ID, API KEY AND SESSION STRING f)
		for reuse storage sessions
	'''

	#EXTRACTING CONFIGURATION (session quantity, api has and api id) 
	with open("config.json","r") as config_file:
		config = json.load(config_file)

	#DEFINING SESSION NUMBER FROM SESSION QUANTITY
	config['session quantity'] += 1
	session_number = config['session quantity']  

	#DEFINING SESSION STORAGE KIND (old session or new from used in DCCU)
	account = "media_downloader" if old_session else ":memory:"

	#START SESSION AND EXTRACTING SESION STRING
	client = pyrogram.Client(account, config['api_id'], config['api_hash']) 
	client.start()
	session = client.export_session_string()

	with open(f'sessions/session{session_number}.txt',"w") as file:
		file.write(session)

	config.update({session_number: 0 }) #zero means ready

	#UPDATING config.json 'session quantity' and 'session_state'
	with open("config.json", "w") as c_file:
		json.dump(config,c_file, indent = 3)

	client.stop()



#FOR CHECK RIGHT STORAGE SESSION INFORMATION
def use_session_string(session_number):
	with open(f'sessions/session{session_number}.txt', "r") as file:
		session_string = file.read()

	with open("config.json","r") as config_file:
		config = json.load(config_file)	
	
	client = pyrogram.Client(session_string, config["api_id"], config["api_hash"]) 
	client.start()

	print(client.get_me().first_name)
	messages = client.iter_history("spotify_down_bot")
	for m in messages:
		print(m.text)	
	client.stop()

if __name__ == "__main__":
	#storage_session_data(old_session = False)
	use_session_string(1)

'''
	Tareas:
	*Annadir estado de las session a config.json
	
	Notas:
	session 0 y 1 son las mismas
'''

