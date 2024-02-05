Descripcion:
El DFS cuenta con tres partes importantes:
Meta-Data:
*registra los distintos datanodes y verifica que no esten duplicados
*despliega todos los archivos en la base de datos
*inserta el file y crea una lista de datanodes posibles para guardar un pedazo
	del file y se los envia a copy
*recupera el size y la informacion de los datanodes y se los envia a copy

data-node:
*conecta los datanodes con el metadata
*recibe desde copy los chunks de un file en sus distntos datanodes
*recupera los chunks de un file y los envia copy

copy: (se llama copy_.py pq me estaba dando un error como si estuviera sobrescribiendo algo)
*se encarga de pasar un file al DFS
	-pueden suceder dos cosas que el file se pueda pasar sin problemas 
	o que se necesite enviar por rango
*se recibe desde los distintos datanode el chunk

colaboradores:
Sergio
Adriana
Bernardo
Nat
Gabriel

