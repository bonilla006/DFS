###############################################################################
#
# Filename: test.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
#       Script to test the MySQL support library for the DFS project.
#
#

# This is how to import a local library
from mds_db import *

# Create an object of type mds_db
db = mds_db("dfs.db") 

# Connect to the database
print("Connecting to database") 
db.Connect() 

# Testing how to add a new node to the metadata server.
# Note that I used a node name, the address and the port.
# Address and port are necessary for connection.

print("Testing node addition")
# id# tiene el id row del nodo que se inserto
############
#esto es el comando reg que va en meta-data?
info_1 = ("136.145.54.10", 80)
id1 = db.AddDataNode(info_1[0], info_1[1]) 
info_2 = ("136.145.54.11", 80)
id2 = db.AddDataNode(info_2[0], info_2[1]) 
print("Testing if node was inserted")
print("A tupple with node name and connection info must appear")
print("nodo 1:", db.CheckNode(info_1[0], info_1[1]))
print("nodo 2:", db.CheckNode(info_2[0], info_2[1]))
############


print("Testing all Available data nodes")
for address, port in  db.GetDataNodes():
	print(address, port)

print("Inserting two files to DB")
#se insertan los files al DFS
db.InsertFile("/hola/cheo.txt", 20)
db.InsertFile("/opt/blah.txt", 30)

#esto es lo que hay que implementar en list en meta-data.py?
print("Choteando one of the steps of the assignment :) ...")
print("Files in the database")
for file, size in db.GetFiles():
	print(file, size)
print

print("Adding blocks to the file, duplicate message if not the first time running")
print("this script")
try:
	db.AddBlockToInode("/hola/cheo.txt", [(id1, "1"), (id2, "2")])
except:
	print("Won't duplicate")
print

print("Testing retreiving Inode info")
fsize, chunks_info = db.GetFileInode("/hola/cheo.txt")
print(chunks_info)
print("File Size is:", fsize)
print("and can be constructed from: ")
for  address, port, chunk in chunks_info:
	print(address, port, chunk)
print

print("Closing connection")
db.Close() 
