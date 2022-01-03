from pathlib import Path
import os
from pydub import AudioSegment
import shutil
import logging
import base64
import dropbox
import json

path = "/home/ariel/Documents/Dayron Work/SingleDownload/singleDownload/test/audio1.mp3"

def convertMediaToOgg(source_dir):
	'''
		Try to convert a media from a given str path and return false if
		could convert the media return the path else return false 
	'''
	base_dir = os.getcwd()
	storage_dir = base_dir + "/temp/" + str(Path(source_dir).stem) + ".ogg"
	if Path(source_dir).suffix == ".mp3":	
		try:
			audio = AudioSegment.from_mp3( source_dir)
		except:
			logging.warning(f'{source_dir} ffmpeg error, file corrupted. Media deleted')
			# os.remove(source_dir)
			return False
		# os.remove(source_dir)

		try:
			audio_converted = audio.export(
						storage_dir, 
						format = "ogg",
						 bitrate="96000",
						 )

			logging.info("{0} converted".format(source_dir))
		except:
			print("Problem ocurr with convertion")
			return False
		#handling duplicate error at converted fold		

		try:
			shutil.move(storage_dir, base_dir + "/converted/")
			return base_dir + "/converted/" + str(Path(source_dir).stem) + ".ogg"
		except:
			logging.info(f'{Path(storage_dir).name} deleted post convertion, already exist in converted folder ')
			os.remove(storage_dir)
			return False
	else:
		logging.error("Error converting, the file to convert isn't .mp3 format")
		return False	


def encodeMediaToBase64(file_path):
	file_path = Path(file_path)
	base_dir =os.getcwd()
	try:
		with open(file_path, "rb") as file:
			data = file.read()
		partial_data_encoded = base64.encodebytes(data[:150])
		
		#concat byte strings
		data_encoded = b''.join([partial_data_encoded,data[150:]])	
		
		encoded_files_paths = base_dir + "/temp/" + file_path.stem + ".txt"
		with open(encoded_files_paths, "wb") as file_to:
			file_to.write(data_encoded)

		shutil.move(encoded_files_paths, base_dir + "/encoded" )
		logging.info(f'{file_path} was encoded')
		os.remove(file_path)
		return base_dir + "/encoded/" + file_path.stem + ".txt"

	except shutil.Error:
		logging.warning(f'{file_path} already being processed, duplicate deleted')
		os.remove(encoded_files_paths)
		os.remove(file_path)
		return False

	except:
		logging.warning(f'{file_path} a problem occur with encode process')
		return False


def uploadToDropbox(file_path, session, index , dropbox_token):
	'''
		upload a single file to dropbox
	'''

	dbx = dropbox.Dropbox(dropbox_token)	
	# for file in files:
	dbx.files_upload(open(file_path, 'rb').read(), f'/Descargas/{sesion}_{index}')


with open("../config.json", "rb") as cfile:
	config = json.load(cfile)
	
logging.basicConfig(level=logging.INFO)
convertion_path = convertMediaToOgg(path)
print(convertion_path)
encode_path = encodeMediaToBase64(convertion_path)


'''
 Recordar:
 *Descomentar para eliminar archivos eliminados
 '''