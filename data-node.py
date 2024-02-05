###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and Yadriel Camis
#
# Description:
# 	data node server for the DFS
#

from Packet import *

import sys
import socket
import socketserver
import uuid
import os.path

def usage():
	print("""Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0]) 
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	# Establish connection
	c_reg = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	c_reg.connect((meta_ip, meta_port))
	# Fill code	

	try:
		response = "NAK"
		sp = Packet()
		while response == "NAK":
			#creas un dicc con commando reg
			sp.BuildRegPacket(data_ip, data_port)
			c_reg.sendall(sp.getEncodedPacket().encode())

			#recibes la confirmacion de si se inserto el datanode o no
			response = c_reg.recv(1024)
			if response == "DUP":
				print("Duplicate Registration")

			if response == "NAK":
				print("Registratation ERROR")

	finally:
		print("esperando...")
		c_reg.close()
	

class DataNodeTCPHandler(socketserver.BaseRequestHandler):

	#funcion que se encarga de recibir los chunks del file 
	#desde copy y acomodarlos en distinos file .data
	def handle_put(self, p):
		#de copy recibes los chunks
		name, size = p.getFileInfo()
		print("Recibiendo el chunk %s"%name)
		# Generates an unique block id.
		dn_ID = str(uuid.uuid1())
		pck = json.dumps(dn_ID)

		#concatenas los chunks enviados por rangos
		#chunk mas grandes que 65k que se envianda de un 
		#rango en especifico
		n_ch = b""
		#max
		MAX = 64000
		#si el chunk es de un size aceptable
		if size < 65536:
			chunk = self.request.recv(size)
			nf = open(dn_ID+".data", "wb")
			nf.write(chunk)
			nf.close()
			
		else:
			#mantiene cuando se a recibido
			leido = 0

			#mientras leido se menor al size implica
			#que falta por acumular bytes
			while leido <= size:
				chunk = self.request.recv(MAX)
				#acumular informacion para asi cuando salga del
				#while pueda escribir el chunk completo
				n_ch += chunk
				
				leido += MAX
			nf = open(dn_ID+".data", "wb")
			nf.write(n_ch)
			nf.close()

		#mandas el uuid de cada datanode a copy
		self.request.send(pck.encode())
		print("Termino de recibir")

	def handle_get(self, p):
		
		# Get the block id from the packet
		dn_ID = p.getBlockID()
		print("Enviando el chunk del file %s:"%dn_ID)

		#enviar el size del archivo con el chunk a copy
		size = os.path.getsize(dn_ID+".data")
		self.request.send(f'{size}'.encode())

		#mandas el uuid de cada datanode al uuid
		#abres el file con el chunk
		r = open(dn_ID+".data", "rb")
		chunk = r.read()
		r.close()

		#mandar el chunk de cada DN a copy
		MAX = 64000
		lim = len(chunk)
		leido = 0
		#controla el rango de envio
		p1 = 0
		p2 = MAX
		if lim < 65536:
			self.request.send(chunk)
		else:
			#dejar mandando hasta que haya completado el chunk
			while leido <= lim:
				self.request.send(chunk[p1:p2])
				p1 = p2
				p2 += MAX
				leido += MAX
		print("Termino de enviar el chunk del file %s:"%dn_ID)

	def handle(self):
		#recibir msg y p desde metadata
		msg = self.request.recv(1024)

		#mandas confirmacion a copy de que se conecto
		self.request.send(b'OK')
		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		
		if cmd == "put":
			self.handle_put(p)

		elif cmd == "get":
			self.handle_get(p)
		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) < 4:
		#print("no hay 4 argumentos")
		usage()

	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		DATA_PATH = sys.argv[3]
		
		if len(sys.argv) > 4:
			META_PORT = int(sys.argv[4])

		#/home/c0t0rrr0/Documents/ccom4017/proyecto/dfs_skel
		if not os.path.isdir(DATA_PATH):
			print("Error: Data path %s is not a directory." % DATA_PATH)
			usage()
	except:
		usage()


	#conexion entre el data-node y el meta-data
	register("localhost", META_PORT, HOST, PORT)
	server = socketserver.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
	server.serve_forever()
