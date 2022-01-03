
with open("uri.txt") as file:
	uris = file.readlines()
	uris = [ i[0:-1] for i in uris] 
	uris = list(set(uris))

with open("cUri.txt", "a") as cfile:
	for uri in uris:
		cfile.write(uri + "\n") 