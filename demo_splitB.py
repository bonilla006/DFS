#############################################
#limita la cantidad de byte que va a poner  #
	#los datanodes                          #
'''num = fsize/cant_DN #cant_DN             #
	read_lim = round(num)                   #
                                            #
	#por si tiene bits extra                #
	extra_b = fsize%cant_DN                 #
                                            #
	fb = open(fname, "rb")                  #
	for chck in range(cant_DN):             #
		list_bin.append(fb.read(read_lim))  #
	if extra_b:                             #
		list_bin.append(fb.read(extra_b))'''#
##############################################

import os
#info del DFS

cant_DN = 3
watch0 = "r1.txt"
watch = "prueba_10b.txt" 
watch2 = "otter.jpg"
watch3 = "WqK1.gif"
watch4 = "Happy Christmas (War Is Over) - John Lennon • Lyrics •.webm"
watch5 = "Hoy jugamos ¡PAREN LA NAVE! Que me bajo.mp4"
watch6 = "Hoy Jugamos Pollo vs. Perrito.mp4"

lee = watch3
fb = open(lee, "rb")

#algoritmo para split bin file
data = []
f_size = os.path.getsize(lee) #35631/3(cant de datanode) = 11877
print("size:", f_size)
read_lim = f_size/cant_DN
extra_b = f_size%cant_DN
print("lim %s" %read_lim)
print("extra %s"%extra_b)
#version1
'''for i in range(cant_DN):
    data.append(fb.read(round(read_lim))) #se esta quedando a mitad porque la division da float
    
#si presenta un residuo la division es pq se esta
#quedando bit fueras en la lectura 
if extra_b:
    #print("hay extra:%s"%extra_b,"<",i,">")
    data.append(fb.read(extra_b))'''

#version2
#buscar como acumular lo que has leido y no pasarte de un cierto valor 65535 = 64k
ind = 1
leido = 0
falta = f_size
vuelta = 1
#esta lista va tener tantos indices como DN hayan conectados
chunk = []
#final_chk = []
#extraaa = b""
MAX = 65535
#primero dividir el file hasta el maximo que puede tener un datanode
while leido < f_size:
    #si esta en la ultima vuelta pon todo en el ultimo DN
    if vuelta == cant_DN:
          print(vuelta)
          #todo lo que falta se puso en el ultimo DN
          chunk.append(fb.read(falta))
          break
    #append los primeros 65k al DN
    chunk.append(fb.read(MAX))

    leido+=MAX
    falta -= MAX
    vuelta+=1
    #print("falta?",falta)

in_DN = []
point = 0
v = 1
ch_u = 0
envio = b""
patita1 = 0
patita2 = MAX
print("el size de donde esta los chunk: %s"%len(chunk))
#se envia 65k para cada datanode
#puede que el ultmio datanode este recibiendo mas de lo que es
for dir in range(cant_DN):
    if dir == len(chunk):
         break
    #print("el size del chunk %s:"%dir,len(chunk[dir]))
    #para enviar el ultimo dn
    if dir == (cant_DN-1):
        #mandar un string b"L" al Ultimo DN
        #y que se quede pegado concatenando los chunck que esta recibiendo
        while point < len(chunk[dir]):
            #se envia de 65k en 65k
            #print("NO SE")
            print("type(%s)"%type(chunk[dir][0:MAX]))   
            print("ANTES p1:",patita1,", p2:",patita2)
            #esto es lo que hay que enviar
            print("\nQUE?%s"%v, chunk[dir][patita1:patita2])
            #concatenas con envio
            envio+=chunk[dir][patita1:patita2]
            patita1 = patita2+1
            patita2 += MAX
            print("DESPUES p1:",patita1,", p2:",patita2)
            v+=1
            point+=MAX
    #envio de manera normal, osea los primeros 65k

       

print("len del file: ", len(chunk))
print("el ultimo chunk es de:", len(chunk[ch_u]))
print("len de envio:", len(envio))
envio_f = input("nombre envio: ")
m = open(envio_f, "wb")
m.write(envio)
m.close
nw_f = b""
chunquis_b = b""
#primero se debe construir cada mini chunk


for i in range(len(chunk)):
	nw_f+=chunk[i]

que_s = "holamundo"
print("muevete:",que_s[0:2])    
#crea un nuevo archivo despues de haber unido los data nodes
file_nuevo = input("nombre: ")
mirador = open(file_nuevo, "wb")
mirador.write(nw_f)
mirador.close

fb.close()

