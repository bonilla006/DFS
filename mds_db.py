###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#

import sqlite3

class mds_db:

	def __init__(self, db_name):
		self.db_name = db_name #dfs_db
		#hace la conexion con la BD
		self.conn = None
		#interactuas con la base de dato
		self.c = None
		#flag para saber si ya se le hizo query
		#para cada nodo en la base de dato se le otorga un visto
		#False = no se insertado
		#True = ya esta insertado
		self.visto = False
		
	#funcion para conectar con la base datos
	def Connect(self):
		"""Connect to the database file"""
		try:
			self.conn = sqlite3.connect(self.db_name)
			self.c = self.conn.cursor()
			self.conn.isolation_level = None
			return 1
		except:
			return 0

	def Close(self): 
		"""Close cursor to the database"""
		try:
			#self.conn.commit()
			self.c.close() 	
			return 1
		except:
			return 0
		
	#funcion para add DN
	def AddDataNode(self, address, port):
		"""Adds new data node to the metadata server
		   Receives IP address and port 
		   I.E. the information to connect to the data node
		"""
		query = """insert into dnode (address, port) values ("%s", %s)""" % (address, port)
		try:
			#inserta en la tabala dnode dos elementos:
			#ip y port
			self.c.execute(query)
			return self.c.lastrowid
		
		except sqlite3.IntegrityError as e: 
			print("ERROR")
			#esta picando el string
			#print(">> ", e.args[0].split()[0])
			#permite duplicados
			if e.args[0].split()[0] == "UNIQUE":
				self.visto = True
				return 0
			else:
				raise
	
	#chequea si item esta en la DB
	def CheckNode(self, address, port):
		"""Check if node is in database and returns name, address, port
                   for connection.
		"""
		query = """select nid from dnode where address="%s" and port=%s""" % (address, port)
		try:
			self.c.execute(query)
		except:
			print("error en checknode: no exite (ip,port)")
			return False
		
		#devulve la fila (id) que matchea con el address y port
		#si se inserto se vio
		return self.c.fetchone()[0]
	
	#verifica si esta duplicado el nodo
	def dupNode(self):
		if self.visto:
			return True
		return False
	
	#devuelve la info que hay en los nodos
	def GetDataNodes(self):
		"""Returns a list of data node tuples (address, port).  Usefull to know to which 
		   datanodes chunks can be send.
		"""
		query = """select address, port from dnode where 1"""
		self.c.execute(query)
		#devuelve una lista de nodos en el DB
		return self.c.fetchall()

	#inserta files a la tabla inode
	def InsertFile(self, fname, fsize):
		"""Create the inode attributes.  For this project the name of the
		   file and its size.
		"""
		query = """insert into inode (fname, fsize) values ("%s", %s)""" % (fname, fsize)
		try:
			self.c.execute(query)
			return 1
		
		#file duplicado
		except:
			print("dup file")
			return 0
	
	#devulve el id y el size del file que se envio
	def GetFileInfo(self, fname):
		"""Given a filename, if the file is stored in DFS
     		   return its filename id and fsize.  Internal use only.
		   Does not have to be accessed from the metadata server.
		"""
		query = """select fid, fsize from inode where fname="%s" """ % fname
		try:
			self.c.execute(query)
			result = self.c.fetchone()
			return result[0], result[1]
		except:
			return None, None

	def GetFiles(self):
		"""Returns the attributes of the files stored in the DFS"""
		"""File Name and Size"""
		query = """select fname, fsize from inode where 1""" ;
		self.c.execute(query)	
		#devulve todos los files
		return self.c.fetchall()

	#relaciona el file con los chunks en los datanodes
	def AddBlockToInode(self, fname, blocks):
		"""Once the Inode was created with the file's attribute
  	           and the data copied to the data nodes.  The inode is 
		   updated to point to the data blocks. So this function receives
                   the filename and a list of tuples with (node id, chunk id)
		"""
		fid, _ = self.GetFileInfo(fname) 
		if not fid:
			return None
		for address, port, chunkid in blocks:
			print("ip:%s"%address,", port:%s"%port,", chkID:",chunkid)
			nid = self.CheckNode(address, port)
			if nid:
				query = """insert into block (nid, fid, cid) values (%s, %s, %s)""" % (nid, fid, chunkid)
				self.c.execute(query)
				print("inserto")
			else:
				print("FALLO INSERT")
				return 0 
		return 1

	#recuperar el 
	def GetFileInode(self, fname):
		"""Knowing the file name this function return the whole Inode information
	           I.E. Attributes and the list of data blocks with all the information to access 
                   the blocks (node name, address, port, and the chunk of the file).
		"""
		fid, fsize = self.GetFileInfo(fname)
		if not fid:
			return None, None
		query = """select address, port, cid from dnode, block where dnode.nid = block.nid and block.fid=%s""" % fid
		self.c.execute(query)
		return fsize, self.c.fetchall() 
		
