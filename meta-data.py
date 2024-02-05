###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz and Yadriel Camis
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
# Please modify globals with appropiate info.

from mds_db import *
from Packet import *
import json
import sys
import socketserver

def usage():
	print("""Usage: python %s <port, default=8000>""" % sys.argv[0]) 
	sys.exit(0)


class MetadataTCPHandler(socketserver.BaseRequestHandler):
	#funcion que se encarga de registrar los datanodes que 
	#van a trabajar en el DFS y verificar que no se conecten
	#duplicados
	def handle_reg(self, db, p):
		print("Registrando DataNode")
		addr = p.getAddr()
		port = p.getPort()
		db.AddDataNode(addr, port) 
		try:
			#si ya esta el nodo va correr este if si no 
			#significa que es la primera vez que se ve
			if db.dupNode():
				print("DataNode duplicado")
				self.request.sendall(b'DUP')
				return
			
			if db.CheckNode(addr, port):
				self.request.sendall(b'ACK') 

			print("Testing all Available data nodes")
			for address, port in  db.GetDataNodes():
				print("(",address,":", port,")")

		except:
			self.request.sendall(b'NAK')

	#funcion para listar todos los files en BD
	def handle_list(self, db):
		print("LIST")
		try:
			print("Archivos:")
			for file, size in db.GetFiles():
				print(file, size)
		except:
			self.request.sendall(b"NAK")	

	#inserta el file a la base de datos y crea una lista
	#de datanodes posibles para enviarselos al cliente
	#y distribuir los chunks del file entre ellos
	def handle_put(self, db, p):
		print("PUT") 
		#lista de datanodes conectados
		l_datanodes = []

		#recuperas nombre y size del file
		info_f = p.getFileInfo()

		#si se logra insertar el file a la BD
		if db.InsertFile(info_f[0], info_f[1]):
			#creas una lista de datanodes para enviarlos a copy
			for address, port in db.GetDataNodes():
				l_datanodes.append((address, port))
			#manda el ip,port de los datanodes
			datanodes = Packet()
			datanodes.BuildPutResponse(l_datanodes)
			self.request.send(datanodes.getEncodedPacket().encode())
		else:
			#el file que se esta insertando ya fue insertado
			self.request.sendall(b'DUP')
	
	def handle_get(self, db, p):
		#recupera el size y la informacion (ip,port,uuid) de los datanodes relacionados con el file
		size, info_DN = db.GetFileInode(p.getFileName()) #[0] = size, [1] = lista de [(ip,port,uuid)]
		
		#si existe size
		if size:
			#crear un paquete para enviarselo a copy
			msg = Packet()
			#lista para guardar 
			l_info = []

			#info_DN = (ip,port,uuid)
			for dn in info_DN:
				l_info.append(dn)

			msg.BuildGetResponse(l_info,size)
			#envias a copy
			self.request.send(msg.getEncodedPacket().encode())
		else:
			self.request.sendall("NFOUND")

	def handle_blocks(self, db, p):
		#recibes un paquete con el path del file y la info de
		#en que parte esta 
		fname = p.getFileName()
		info_chunk = p.getDataBlocks()

		# Fill code to add blocks to file inode
		#envia a la base de dato el path del file en la BD
		#y el info del datanode
		db.AddBlockToInode(fname, info_chunk)
		
	def handle(self):
		#se establece una comunicacion con la BD
		db = mds_db("dfs.db")
		db.Connect()

		#se crea un objeto paquete para poder interactuar con los comandos
		#del cliente
		p = Packet()

		#recibe los mensajes
		msg = self.request.recv(1024)
		
		#abre el paquete
		p.DecodePacket(msg)
		cmd = p.getCommand()

		#controla si va a registrar DN, list, insertar chunks en el DFS
		#o recuperar chunks del DFS
		if cmd == "reg":
			#registra el DN
			self.handle_reg(db, p)

		elif cmd == "list":
			#lista de file en BD
			self.handle_list(db)
		
		elif cmd == "put":
			#poner en DFS
			self.handle_put(db, p)

		elif cmd == "get":
			#recuperar del DFS
			self.handle_get(db,p)

		elif cmd == "dblks":
			#enlaza los chunks y uuid con los DN conectados
			#a MD
			self.handle_blocks(db,p)

		db.Close()

if __name__ == "__main__":
    HOST, PORT = "localhost", 8000

#se verifica que tenga minimo un argumento, localhost
if len(sys.argv) > 1:
	try:
		PORT = int(sys.argv[1])
	except:
		usage()

server = socketserver.TCPServer((HOST, PORT), MetadataTCPHandler)

# Activate the server; this will keep running until you
# interrupt the program with Ctrl-C
server.serve_forever()#datanode.py
