###############################################################################
#
# Filename: Packet.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Packet creation support library for the DFS project. Database info for
#
# Please modify globals with appropiate info.

import json

class Packet:

	def __init__(self):
	
		self.commands = ["reg", "list", "put", "get", "dblks"]
		#el diccionario tiene:
		#command, addr, port, files, fname, fsize, blocks, blockid, server
		self.packet = {}

	#encode la info que se quiere enviar
	def getEncodedPacket(self):
		return json.dumps(self.packet)

	#recuperas el comando del paquete
	def getCommand(self):
		"""Returns the command type of a packet"""
		if "command" in self.packet:
			return self.packet["command"]
		return None

	#recuperas el ip del DN
	def getAddr(self):
		"""Returns the IP address of a server""" 
		if "addr" in self.packet:
			return self.packet["addr"]
		return None
	
	#recupearas el port del DN
	def getPort(self):
		"""Returns the port number of a server"""
		if "port" in self.packet:
			return self.packet["port"]
		return None

	#decodificas el paquete recibido
	def DecodePacket(self, msg):
		"""Receives a serialized message and turns it into a packet object."""
		try:
			self.packet = json.loads(msg)	
			return self.packet	
		except:
			print("error en decode")

	#funciones BUILD, para crear paquete
	#ip y port del data-node
	def BuildRegPacket(self, addr, port):
		"""Builds a registration packet"""
		self.packet = {"command": "reg", "addr": addr, "port": port}
		
	#LISTA
	def BuildListPacket(self):
		"""Builds a list packet for file listing"""
		#self.packet = {"command": "reg", "addr": addr, "port": port}
		self.BuildCommand("list")
	def BuildListResponse(self, lfiles):
		"""Builds a list response packet"""
		self.packet = {"files": lfiles}	

	#PUT
	#el nombre del file en la computadora y su size
	def BuildPutPacket(self, fname, fsize):
		"""Builds a put packet to put fname and file size."""
		self.BuildCommand("put")
		#print("esto es el fname:%s" %fname)
		self.packet["fname"] = fname
		#print("esto esta en el dicc:%s" %self.packet["fname"])
		self.packet["fsize"] = fsize

	#lista de datanodes
	def BuildPutResponse(self, metalist):
		"""Builds a list of data node servers where a file data blocks can be stored.
		I.E. a list of available data servers."""
		self.packet["servers"] = metalist
	#chunks<-->uuids
	def BuildDataBlockPacket(self, fname, block_list):
		"""Builds a data block packet. Contains the file name and the list of blocks for the file"""
		self.BuildCommand("dblks")
		self.packet["fname"] = fname
		self.packet["blocks"] = block_list

	#GET
	#path del file en la BD
	def BuildGetPacket(self, fname):
		"""Build a get packet to get fname."""
		self.BuildCommand("get")
		self.packet["fname"] = fname
	#uuid
	def BuildGetDataBlockPacket(self, blockid):
		"""Builds a get data block packet. Usefull when requesting a data block to a data node."""
		self.BuildCommand("get")
		self.packet["blockid"] = blockid
	#
	def BuildGetResponse(self, metalist, fsize):
		"""Builds a list of data node servers with the blocks of a file, and file size."""
		self.packet["servers"] = metalist
		self.packet["fsize"] = fsize

	#el key command se le asigna el value que se le pase
	def BuildCommand(self, cmd):
		"""Builds a packet type"""
		if cmd in self.commands:
			self.packet = {"command": cmd}

	#===========================================================		
	#funciones GET, recuperas info del paquete
	def getFileArray(self):
		"""Builds a list response packet"""

		if "files" in self.packet:
			return self.packet["files"]

	#recupera los block
	def getBlockID(self):
		"""Returns a the block_id from a packet."""
		return self.packet["blockid"]

	#devuelve el nombre y el size
	def getFileInfo(self):
		"""Returns the file info in a packet."""
		if "fname" in self.packet and "fsize" in self.packet:
			return self.packet["fname"], self.packet["fsize"]
		else:
			print("FALLO en getFileInfo")
	
	#recupera el nombre	
	def getFileName(self):
		"""Returns the file name in a packet."""
		if "fname" in self.packet:
			return self.packet["fname"] 

	def getDataNodes(self):
		"""Returns a list of data servers"""
		if "servers" in self.packet:
			return self.packet["servers"]
		return None

	def getDataBlocks(self):
		"""Returns a list of data blocks""" 
		if "blocks" in self.packet:
			return self.packet["blocks"]
		return None

	



		
	
		
		