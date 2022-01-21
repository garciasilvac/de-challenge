Desafío para Cargo de Data Engineer
Candidato: Carlos García Silva


Consideraciones:

0. Se desarrolla esta solución en un Macbook Air M1.
1. Se decide utilizar Python 3.8 como lenguaje de programación
2. Se decide utilizar Spark 3.2.0 como herramienta de ETL (Spark SQL) y pyspark como libreria para utilizar Spark en python. Esto dada la naturaleza del problema y del cargo al cual se postula. Spark permite manejar datos distribuidos en distintos servidores de forma segura y rápida cuando se trata de grandes volumenes de datos. Es mejor versus otras soluciones como MapReduce, dado que maneja los datos en memoria y MapReduce en disco, lo que incrementa la rapidez de las consultas y otras operaciones, segun contenidos vistos en el diplomado que actualmente curso sobre Big Data para la toma de decisiones.
3. Para el set up: Se instala python 3.8 utilizando Brew, Se instala pyspark utilizando pip. Se configura algunas ruta para java home y spark home para le uso de PySpark.
3. Se decide agregar libreria findspark para poder encontrar la ruta a spark facilmente.
4. Se decide utilizar un script ya que el proceso de ETL es por definicion lineal (Job): extract, transform, load.
5. En la etapa Extract: se lee los archivos csv que fueron otorgados como input del problema (dataset)
6. En la etapa Transform: se limpian los datos (espacios en palabras, palabras en datos numéricos), se normaliza la base, se procede a calcular los indicadores solicitados
7. En la etapa Load: se prepara el output (se se imprimen en pantalla, se entendería que despues esta información podría ser enviada mediante una API a los servidores del área de Analytics. Se crean archivos JSON con los reportes)
8. Se usa logica de data pipelines para hacer los cálculos:
    a: Sobre la base normalizada se calculan los promedios por juego/consola (agregando las fechas)
    b: se calcula TOP 10 y WORST 10 por consola.
    c: Entendiendo la relación entre consola y compañia, se calculan los TOP 10 y WORST 10 por compañia usando los TOP ya procesados para cada consola
    d: EL TOP Y WORST DE TODOS LOS JUEGOS van a ser los top y worst de los subconjuntos que los contienen, por lo que se toma los top y worst de las compañias para calcular os nuevos TOP y WORST GLOBALES.

    usando esta lógica se ahorra tiempo de computo, aprovechando los calculos anteriores.

Deployment:

Se debe ejecutar el archivo main.py desde a ubicacion de-challenge/Deployment/. Es necesario estar ubicado en esa ruta para la correcta lectura de las rutas de acceso a los archivos input del problema.