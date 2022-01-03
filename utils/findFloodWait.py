import pyrogram
import json
import jsonpickle
from pyrogram.errors import FloodWait
from time import sleep

def use_session_string(session_number):
	with open(f'sessions/session{session_number}.txt', "r") as file:
		session_string = file.read()

	with open("config.json","r") as config_file:
		config = json.load(config_file)	
	
	client = pyrogram.Client(session_string, config["api_id"], config["api_hash"]) 
	client.start()

	print(client.get_me().first_name)
	i=0
	while True:
		try:
			print(f'Attemp {i}')
			messages = client.iter_history("spotify_down_bot")
			sleep(3)
			i+=1
		except FloodWait as e:
			print(f'FloodWait rate {i}')
			print(f'Floot time {e.x}')


if __name__ == "__main__":
	#storage_session_data(old_session = False)
	use_session_string(6)