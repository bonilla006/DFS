###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	List client for the DFS
#



import socket
import sys
from Packet import *

def usage():
	print("""Usage: python %s <server>:<port, default=8000>""" % sys.argv[0]) 
	sys.exit(0)

def client(ip, port):

	# Contacts the metadata server and ask for list of files.
	#establece una conecion con metadata
	c_list = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	c_list.connect((ip, port))

	#crear un paquete
	list_file = Packet()
	
	#crear un dicc con comando list 
	list_file.BuildListPacket()
	
	#mandas el paquete encoded a metadata
	c_list.sendall(list_file.getEncodedPacket().encode())

	return 0
if __name__ == "__main__":

	if len(sys.argv) < 2:
		print("no 2")
		usage()

	#ip = sys.argv[]
	#port = None 
	#server = sys.argv[1].split(":")
	print(sys.argv[1])
	if len(sys.argv) == 2:
		print("un argv")
		ip = sys.argv[1]
		port = 8000

	elif len(sys.argv) == 3:
		print("dos argv")
		ip = sys.argv[1]
		port = sys.argv[2]#int server[1]
	
	if not ip:
		print("no ip")
		usage()

	client(ip, port)
