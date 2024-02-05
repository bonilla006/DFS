###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and Yadriel Camis
#
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path
from Packet import *

def usage():
	print("""Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   
	   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0]))
	sys.exit(0)

#funcion para pasar un file de tu computadora a el DFS
def copyToDFS(address, fname, path):
	print("Copiando al DFS")
	#sacas el size del file que quieres colocar
	fsize = os.path.getsize(fname)
	
	#establece una conexion con metadata
	c_MD = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	c_MD.connect((address[0], address[1]))
    
	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 
    #crear un paquete
	copy_put = Packet()
	#crear un paquete con comando put 
	copy_put.BuildPutPacket(path, fsize)
	#mandas el paquete encoded a metadata
	c_MD.sendall(copy_put.getEncodedPacket().encode())

	#====================================================
	# algoritmo para dividir file
	#size del file
	'''fsize'''
	#recibes los datanodes conectados(ip,port) desde metadata
	msg_DN = c_MD.recv(1024)
	data = msg_DN.decode()
	d_msg_DN = json.loads(data) #tupple con addr y port de los datanode
	#tomas la lista de datanodes y la evaluas en la funcion lambda que va a 
	#convertir la lista en tuple
	l_DN = list(map(lambda x:tuple(x), d_msg_DN["servers"]))
	
	#cantidad de datanode
	cant_DN =  len(l_DN)
	#marca el limite que se va tener por chunk
	num = fsize/cant_DN
	lim = round(num)
	#por si se quedo fuera algun chunk
	extra_ch = fsize%cant_DN
	#esta lista va tener los chunks
	chunks = []
	fb = open(fname, "rb") 
	for ch in range(cant_DN):
		chunks.append(fb.read(lim))
	if extra_ch:
		chunks[len(chunks)-1]+=fb.read(extra_ch)
	fb.close() 
	#====================================================
	#limite de envio
	MAX = 64000

	#se debe crear un packete put, guarda los chunks del file
	chunk_file = Packet()

	#paquete que envia a metadata la info del file
	chunk_info = Packet()

	#guarda toda la info por indice
	#ip,port y uuid por indice
	info_file = []

	#recorrer la lista de datanodes para enviar el chunck por datanode	
	for ind, dn in enumerate(l_DN):
		#conexion con los distintos datanode
		c_DN = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		c_DN.connect((dn[0], dn[1]))

		#creas el pack para enviar el chunck del archivo que corresponde por datanode
		#a los distintos datanodes
		chunk_file.BuildPutPacket(ind, lim) 
		#empieza la comunicacion con los DN
		c_DN.send(chunk_file.getEncodedPacket().encode())

		#recibes el ok del DN
		r_DN = c_DN.recv(1024).decode()
		print("<<<COMENZANDO PROCESO DE ENVIO A LOS Data Nodes>>>")
		
		#el size del chunk es menor a 65k
		#pueden enviarse sin ningun problema
		if lim < 65536:
			#envias el chunk a cada DN
			c_DN.send(chunks[ind])

		#el size del chunk es mayor a 65k
		#por chunk se va a enviar en un rango de 65k
		else:
			#indica cuanto a leido del chunk
			leido = 0
			#controlan el rango de envio
			p1 = 0
			p2 = MAX
			while leido <= len(chunks[ind]):
				c_DN.send(chunks[ind][p1:p2])
				p1 = p2
				p2 += MAX
				leido += MAX
			#termino el while implica que se paso el chunk completo al DN
			
		#recibes los uuid de los distintos datanode
		uuid = c_DN.recv(1024).decode()

		#llenar la info del file
		info_file.append((dn[0],dn[1],uuid))
		#ind+=1

	#mandar con path y uuid un comando blks a metadata para buscar el archivo
	#path,[lista de bloques]
	#establece una conecion con metadata
	c_chunk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	c_chunk.connect((address[0], address[1]))
    
	#envias la informacion del file, en que DN esta ubicado el chunck 
	#junto con el path en el DFS
	chunk_info.BuildDataBlockPacket(path, info_file)
	c_chunk.sendall(chunk_info.getEncodedPacket().encode())
	
#recuperar del DFS
def copyFromDFS(address, fname, path):
	print("Recuperando del DFS")
   	# Contact the metadata server to ask for information of fname
	#establece una conecion con metadata
	c_copy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	c_copy.connect((address[0], address[1]))
    
    #crear un paquete para enviar get
	copy_get = Packet()
	#mandar el path del file 
	copy_get.BuildGetPacket(path)
	#mandas el paquete encoded a metadata
	c_copy.sendall(copy_get.getEncodedPacket().encode())

	#recibes la info de datanodes(ip,port,uuid) desde metadata
	msg_uuid = c_copy.recv(1024)
	data = msg_uuid.decode()
	d_msg_uuid = json.loads(data) #tupple con addr y port de los datanode

	#tomas la lista de datanodes/uuid y la evaluas en la funcion lambda que va a convertir
	#la lista en tuple
	info = list(map(lambda x:tuple(x), d_msg_uuid["servers"]))
	
	#paquete para enviar el uuid a cada datanode
	uuid = Packet()
	data = []
	
	#acumulador de chunks pasados desde los DN
	r_ch = b""
	#
	chunks = b""
	#
	MAX = 64000
	for ind,dn in enumerate(info):
		#conectar con datanodes
		c_DN = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		c_DN.connect((dn[0], dn[1]))
		#enviar el uuid al datanode
		uuid.BuildGetDataBlockPacket(dn[2])
		c_DN.sendall(uuid.getEncodedPacket().encode())

		#recibes el ok del DN
		r_DN = c_DN.recv(1024).decode()

		#recibes el size de los chunks
		size = c_DN.recv(1024).decode()

		#recibes los chunks de los distintos datanode
		if int(size) < 65536:
			r_ch = c_DN.recv(int(size))
		else:
			leido = 0
			r_ch = b""
			while leido <= int(size):
				binarios = c_DN.recv(MAX)
				r_ch += binarios
				leido += MAX
		###
		chunks = r_ch
		data.append(chunks)

	nf = b""
	for c in data:
		nf += c
    # Save the file
	mirador = open(fname, "wb")
	mirador.write(nf)
	mirador.close()

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")
	
	###################################################################
	#si es mas de un arg en file to implica que estas pasando un file
	#al DFS
	#python3 copy_.py r1.txt localhost:8000:/dfs/file.txt
	if len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = sys.argv[1] 
		from_path = file_to[2]

		if os.path.isdir(from_path):
			print("Error: path %s is a directory.  Please name the file." % from_path)
			usage()
			
		copyToDFS((ip, port), to_path, from_path)
	#####################################################################
	#del DFS a la computadora
	#si es mas de un arg en file_from implica que estas sacandod
	#del dfs
	#python3 copy_.py localhost:8000:/dfs/r1.txt n_file.txt
	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print("Error: path %s is a directory.  Please name the file." % to_path)
			usage()
		
		copyFromDFS((ip, port), to_path, from_path)
	###################################################################
	


