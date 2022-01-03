import pymongo
import urllib.parse
import random
import datetime
       
# TO PASS USER AND PASS IN MONGO DB LINK 
user = urllib.parse.quote_plus('lyra')
print(user)
password = urllib.parse.quote_plus('HLR5Dm2dspAL3Gr') 
print(password)
print(f'mongodb://{user}:{password}@127.0.0.1:27017')
      
#print(datetime.datetime.now())

