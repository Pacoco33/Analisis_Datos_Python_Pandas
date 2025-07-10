>>> df = spark.read.csv('s3://paco-cubo/frutas_100mb.csv',header=True)
>>> df.show(100)                                                                
+---+-------+--------+-----------+-------------+-----------------+--------+---------+-----------+----------+
| id| nombre|   color|      sabor|peso_promedio|calorias_por_100g|  origen|temporada|es_tropical| vitaminas|
+---+-------+--------+-----------+-------------+-----------------+--------+---------+-----------+----------+
|  1|   Pera|    NULL|       NULL|         NULL|            106.5|   China|    Otoño|       True|      E;B2|
|  2|  Fresa|    Rojo|       NULL|       959.16|             81.1|    NULL|   Verano|      False| B1;D;B2;C|
|  3|Durazno|    NULL|     Amargo|         NULL|             93.5|    Perú| Invierno|       True|       K;A|
|  4|   NULL|    NULL|      Dulce|       376.62|             67.5|    NULL|    Otoño|      False|      NULL|
|  5| Banana|   Verde|     Amargo|       967.74|            118.8|   China|     NULL|      False|         C|
|  6| Sandía|    Rosa|Astringente|         NULL|             83.8|   India|   Verano|       True|         K|
|  7|   NULL|    NULL|     Amargo|       629.86|             NULL|  México|   Verano|       True|      NULL|
|  8|   Piña|Amarillo|     Neutro|       373.15|            107.8|  Brasil|    Otoño|       True|   B1;B2;K|
|  9|   NULL|  Morado|Astringente|         NULL|            108.0|    NULL|     NULL|      False|      NULL|
| 10|   Piña|    Rosa|       NULL|       215.75|             91.2|  México|Primavera|       NULL|      NULL|
| 11| Cereza|    Rojo|       NULL|       618.77|             NULL|  España|    Otoño|       True| B1;B6;K;A|
| 12|   NULL|    Rojo|      Ácido|       467.64|             NULL|   India|     NULL|      False|      NULL|
| 13|   Piña| Naranja|      Dulce|        51.81|             35.5|Colombia|     NULL|       True|      C;B6|
| 14|  Fresa|   Verde|       NULL|       528.66|             87.4|  España|     NULL|      False| A;B1;E;B2|
| 15|   Pera|   Verde|Astringente|         NULL|             62.7|Colombia|     NULL|       True|         A|
| 16|  Fresa|    Rosa|Astringente|        911.5|             55.6|   Chile|   Verano|      False|         E|
| 17|    Uva|   Verde|     Amargo|       278.71|            101.3|   Chile|     NULL|       True|   E;K;D;C|
| 18|   Lima|    Rojo|     Amargo|       541.41|            141.3|   Chile|Primavera|       True|  E;B6;K;C|
| 19|   NULL|   Verde|Astringente|        76.62|            128.5|Colombia|   Verano|      False|         K|
| 20| Sandía| Naranja|       NULL|       752.08|            117.0|   Chile|Primavera|      False|        B6|
| 21|Durazno|  Morado|     Amargo|         NULL|            129.3|    NULL|     NULL|      False| E;K;B6;B2|
| 22| Banana|Amarillo|     Neutro|       338.49|             NULL|Colombia| Invierno|       True|   E;C;K;A|
| 23|   Lima|    Rosa|     Neutro|         NULL|            127.2|    NULL| Invierno|       True|        B6|
| 24|Ciruela| Naranja|Astringente|       721.99|             NULL|    NULL|    Otoño|      False|  A;K;C;B6|
| 25| Banana| Naranja|Astringente|       615.23|             56.0|   China|   Verano|       True|   E;B6;B1|
| 26| Cereza|    Rosa|Astringente|       164.54|             40.8|   China|Primavera|       True|         E|
| 27| Cereza|    NULL|       NULL|       346.25|             63.3|   India| Invierno|       True|    K;B2;D|
| 28| Papaya|  Morado|Astringente|       728.43|             90.2|   Chile|    Otoño|       NULL|  C;B6;E;D|
| 29| Sandía|   Verde|     Amargo|       124.14|            129.3|   Chile| Invierno|      False|  B6;K;A;D|
| 30|Durazno|  Morado|     Neutro|       636.47|             31.0|    NULL| Invierno|       NULL| K;B6;D;B2|
| 31|  Mango|    Rojo|       NULL|       184.47|             NULL|  Brasil|Primavera|      False|        B1|
| 32| Papaya|Amarillo|      Ácido|       502.27|             NULL|  Brasil|     NULL|      False|     B1;B6|
| 33| Sandía| Naranja|Astringente|       341.89|             66.1|  España| Invierno|      False|      NULL|
| 34|  Mango| Naranja|       NULL|        167.2|            121.4|  Brasil|Primavera|       True|         K|
| 35|   NULL|   Verde|       NULL|        67.13|            119.2|    NULL|Primavera|      False|      NULL|
| 36|  Fresa| Naranja|     Neutro|       107.49|             92.6|Colombia|     NULL|       True|  E;B1;C;K|
| 37|    Uva|    Rojo|     Amargo|       314.59|             NULL|Colombia|     NULL|      False|        B1|
| 38|   Piña|   Verde|       NULL|        717.0|             NULL|  México|Primavera|      False|    C;B6;D|
| 39|  Melón|    Rosa|Astringente|       687.36|            113.1|    NULL|   Verano|       NULL|      NULL|
| 40|    Uva|  Morado|       NULL|       475.86|             NULL|    NULL| Invierno|      False|         K|
| 41|   Kiwi|    NULL|      Dulce|       827.87|             NULL|Colombia|Primavera|      False| E;B2;B6;K|
| 42| Banana|    NULL|      Dulce|        440.5|            100.0|   Chile|     NULL|       NULL|       A;D|
| 43|   Kiwi|    Rojo|Astringente|         NULL|             87.6|    Perú|     NULL|       True|      NULL|
| 44|   NULL|    Rojo|      Dulce|       331.93|            116.1|Colombia|     NULL|       True|     E;A;C|
| 45|  Limón|   Verde|     Amargo|       755.36|             57.2|    Perú|Primavera|       True|     E;K;A|
| 46|  Melón|    Rojo|     Neutro|        878.0|            102.8|  Brasil|   Verano|       True|   K;C;D;A|
| 47|  Fresa|   Verde|      Ácido|         NULL|            141.4|    NULL|Primavera|      False|   A;B2;B6|
| 48| Cereza|    Rojo|       NULL|       289.15|             76.8|    NULL|   Verano|      False|  K;B2;D;C|
| 49| Papaya|   Verde|     Amargo|       220.35|             NULL|  España|    Otoño|       NULL|     B2;B6|
| 50| Papaya| Naranja|      Dulce|       814.93|             65.9|   India|    Otoño|      False|      NULL|
| 51| Cereza|    Rojo|     Neutro|       719.63|             39.6|  México|   Verano|       True|     C;E;K|
| 52|   NULL|  Morado|Astringente|        58.09|            114.8|  Brasil|   Verano|       True|      K;B1|
| 53|  Melón|    Rojo|      Ácido|       368.92|            129.2|  España|    Otoño|       NULL|     B2;B6|
| 54|  Mango|    Rojo|     Amargo|         NULL|             77.2|    NULL|    Otoño|       True|      A;B6|
| 55|  Fresa|Amarillo|Astringente|       375.26|             72.0|   India|Primavera|       NULL|        B2|
| 56|  Limón|   Verde|       NULL|       674.01|             37.7|   India|Primavera|       True|         D|
| 57|   NULL|   Verde|Astringente|       725.72|             74.9|   China| Invierno|       NULL|      B1;A|
| 58|Manzana|Amarillo|Astringente|         NULL|            114.3|    Perú|    Otoño|       NULL|      B2;K|
| 59| Papaya|  Morado|Astringente|         NULL|            101.4|   India|     NULL|       True|      NULL|
| 60| Sandía|Amarillo|     Amargo|       622.29|            123.5|   China|   Verano|       True|      NULL|
| 61|   Piña|    Rojo|Astringente|        70.25|            113.1|  España|     NULL|      False|        B6|
| 62|   NULL|    Rojo|      Ácido|       188.07|             65.2|    NULL|    Otoño|       True|  E;K;D;B6|
| 63|   Kiwi|    Rojo|Astringente|         NULL|             48.3|   China|   Verano|       NULL|    A;E;B1|
| 64|   Lima|    Rosa|      Dulce|       465.61|            118.2|  México|     NULL|       True|    A;C;B6|
| 65|   Lima|  Morado|      Ácido|       304.55|            107.3|   India|    Otoño|       NULL|      B6;D|
| 66|   NULL|    NULL|     Neutro|        887.0|            142.1|    NULL|    Otoño|       NULL|      A;B2|
| 67| Banana| Naranja|     Neutro|       543.51|            127.4|  Brasil|Primavera|      False|      NULL|
| 68|Ciruela|   Verde|     Neutro|       524.12|            131.1|   Chile|    Otoño|       True|        B1|
| 69| Papaya|   Verde|      Dulce|       166.36|            104.3|   China|    Otoño|       True|      NULL|
| 70|   Lima|    NULL|      Dulce|       320.43|            116.1|   China| Invierno|       NULL|   B1;B6;A|
| 71|Durazno| Naranja|       NULL|       497.14|            111.6|   India|   Verano|      False|         E|
| 72|  Fresa|    Rosa|Astringente|       270.58|            118.2|Colombia| Invierno|       NULL|         D|
| 73|   NULL|Amarillo|      Ácido|       111.72|            113.4|    NULL|    Otoño|      False|     B1;B2|
| 74|  Melón|  Morado|     Amargo|       742.35|             NULL|   Chile|Primavera|       True|  D;A;B1;C|
| 75|   Piña|    NULL|     Neutro|       121.87|             93.2|  México|   Verano|       True| A;B2;B1;D|
| 76|   Piña|  Morado|       NULL|         NULL|             86.5|  Brasil|    Otoño|      False|  E;B6;C;K|
| 77| Papaya|Amarillo|      Dulce|       375.75|            145.3|   India|Primavera|      False|      NULL|
| 78| Papaya|    NULL|       NULL|       354.64|             NULL|   China|Primavera|       True|   B1;A;B6|
| 79| Sandía|    Rosa|      Ácido|       737.42|            111.7|   China|     NULL|       True|     D;K;A|
| 80|Durazno| Naranja|     Neutro|       524.16|            119.0|Colombia| Invierno|       NULL|         D|
| 81| Cereza|    Rojo|      Ácido|       387.03|             77.6|  Brasil|     NULL|       True|      NULL|
| 82|   NULL|  Morado|Astringente|       693.41|            143.9|Colombia|   Verano|       True|      NULL|
| 83|   Lima|    NULL|     Neutro|        66.75|             88.0|Colombia|     NULL|      False|         E|
| 84|  Fresa| Naranja|       NULL|       127.28|             NULL|Colombia| Invierno|       True|      NULL|
| 85| Papaya|    NULL|     Amargo|       220.31|             30.7|   China|     NULL|      False|      NULL|
| 86| Sandía|Amarillo|     Neutro|         NULL|             44.9|   Chile|     NULL|       NULL|    C;D;B6|
| 87| Banana|   Verde|      Ácido|       908.94|             66.3|  México|   Verano|      False|        B2|
| 88|   Pera|    Rosa|     Amargo|         NULL|             99.6|Colombia|Primavera|       NULL|    B2;K;E|
| 89| Banana|    Rosa|     Neutro|       543.64|             77.4|    NULL| Invierno|       True|      B1;D|
| 90|   NULL|   Verde|      Ácido|       225.49|             34.8|  México|     NULL|       NULL|   A;B2;B1|
| 91|Ciruela|    Rosa|       NULL|         NULL|            129.6|   China|   Verano|       True|      B6;C|
| 92|  Melón|Amarillo|      Ácido|         NULL|             NULL|    NULL|Primavera|      False|      NULL|
| 93| Banana|    Rojo|     Neutro|       332.96|            138.6|    NULL|    Otoño|      False|         D|
| 94|Durazno|    NULL|       NULL|       868.29|            123.6|    NULL|    Otoño|      False|B1;D;B6;B2|
| 95|Ciruela|Amarillo|     Neutro|       748.84|             NULL|Colombia|Primavera|      False|     B2;B1|
| 96|  Mango|    Rosa|     Amargo|        98.47|             87.4|    NULL|    Otoño|       NULL|      NULL|
| 97|  Mango|    Rojo|     Neutro|       867.93|             42.5|  México| Invierno|       True| B1;B2;C;A|
| 98|  Melón|  Morado|Astringente|       702.81|            128.8|Colombia|Primavera|       NULL|      NULL|
| 99| Cereza|    Rojo|      Ácido|       628.94|            144.5|  España|Primavera|       True| A;K;B6;B2|
|100|Durazno|  Morado|     Amargo|       904.58|             42.3|Colombia|Primavera|       True|        B1|
+---+-------+--------+-----------+-------------+-----------------+--------+---------+-----------+----------+
only showing top 100 rows

>>> df.printSchema()
root
 |-- id: string (nullable = true)
 |-- nombre: string (nullable = true)
 |-- color: string (nullable = true)
 |-- sabor: string (nullable = true)
 |-- peso_promedio: string (nullable = true)
 |-- calorias_por_100g: string (nullable = true)
 |-- origen: string (nullable = true)
 |-- temporada: string (nullable = true)
 |-- es_tropical: string (nullable = true)
 |-- vitaminas: string (nullable = true)

>>> from pyspark.sql.functions import col, when, count
>>> 
>>> total_rows = df.count()
>>>                                                                             
>>> null_counts = df.select([
...     count(when(col(c).isNull(), c)).alias(c)
...     for c in df.columns
... ]).collect()[0]
>>>                                                                             
>>> for col_name in df.columns:
...     n_nulls = null_counts[col_name]
...     print(f"{col_name}: {n_nulls} nulos")
... 
id: 0 nulos
nombre: 400251 nulos
color: 399714 nulos
sabor: 399421 nulos
peso_promedio: 399340 nulos
calorias_por_100g: 399434 nulos
origen: 400041 nulos
temporada: 400544 nulos
es_tropical: 400548 nulos
vitaminas: 400201 nulos
>>> df = df.na.drop()
>>> 
>>> total_rows = df.count()
>>>                                                                             
>>> null_counts = df.select([
...     count(when(col(c).isNull(), c)).alias(c)
...     for c in df.columns
... ]).collect()[0]
>>>                                                                             
>>> for col_name in df.columns:
...     n_nulls = null_counts[col_name]
...     print(f"{col_name}: {n_nulls} nulos")
... 
id: 0 nulos
nombre: 0 nulos
color: 0 nulos
sabor: 0 nulos
peso_promedio: 0 nulos
calorias_por_100g: 0 nulos
origen: 0 nulos
temporada: 0 nulos
es_tropical: 0 nulos
vitaminas: 0 nulos
>>> 
>>> df.select("nombre", "color", "sabor").show(10)
+-------+--------+-----------+                                                  
| nombre|   color|      sabor|
+-------+--------+-----------+
|   Piña|Amarillo|     Neutro|
|  Fresa|    Rosa|Astringente|
|   Lima|    Rojo|     Amargo|
| Banana| Naranja|Astringente|
| Cereza|    Rosa|Astringente|
| Sandía|   Verde|     Amargo|
|  Limón|   Verde|     Amargo|
|  Melón|    Rojo|     Neutro|
| Cereza|    Rojo|     Neutro|
|Ciruela|   Verde|     Neutro|
+-------+--------+-----------+
only showing top 10 rows

>>> 
>>> df.filter(df["es_tropical"] == "True").show(10)
+---+-------+--------+-----------+-------------+-----------------+------+---------+-----------+---------+
| id| nombre|   color|      sabor|peso_promedio|calorias_por_100g|origen|temporada|es_tropical|vitaminas|
+---+-------+--------+-----------+-------------+-----------------+------+---------+-----------+---------+
|  8|   Piña|Amarillo|     Neutro|       373.15|            107.8|Brasil|    Otoño|       True|  B1;B2;K|
| 18|   Lima|    Rojo|     Amargo|       541.41|            141.3| Chile|Primavera|       True| E;B6;K;C|
| 25| Banana| Naranja|Astringente|       615.23|             56.0| China|   Verano|       True|  E;B6;B1|
| 26| Cereza|    Rosa|Astringente|       164.54|             40.8| China|Primavera|       True|        E|
| 45|  Limón|   Verde|     Amargo|       755.36|             57.2|  Perú|Primavera|       True|    E;K;A|
| 46|  Melón|    Rojo|     Neutro|        878.0|            102.8|Brasil|   Verano|       True|  K;C;D;A|
| 51| Cereza|    Rojo|     Neutro|       719.63|             39.6|México|   Verano|       True|    C;E;K|
| 68|Ciruela|   Verde|     Neutro|       524.12|            131.1| Chile|    Otoño|       True|       B1|
| 97|  Mango|    Rojo|     Neutro|       867.93|             42.5|México| Invierno|       True|B1;B2;C;A|
| 99| Cereza|    Rojo|      Ácido|       628.94|            144.5|España|Primavera|       True|A;K;B6;B2|
+---+-------+--------+-----------+-------------+-----------------+------+---------+-----------+---------+
only showing top 10 rows

>>> 
>>> df.filter(df["peso_promedio"].cast("float") > 500).select("nombre", "peso_promedio").show(10)
+-------+-------------+
| nombre|peso_promedio|
+-------+-------------+
|  Fresa|        911.5|
|   Lima|       541.41|
| Banana|       615.23|
|  Limón|       755.36|
|  Melón|        878.0|
| Cereza|       719.63|
|Ciruela|       524.12|
| Banana|       908.94|
|  Mango|       867.93|
| Cereza|       628.94|
+-------+-------------+
only showing top 10 rows

>>> 
>>> df.filter(df["vitaminas"].contains("A")).select("nombre", "vitaminas").show(10)
+------+---------+
|nombre|vitaminas|
+------+---------+
|Sandía| B6;K;A;D|
| Limón|    E;K;A|
| Melón|  K;C;D;A|
| Mango|B1;B2;C;A|
|Cereza|A;K;B6;B2|
| Limón|D;B2;A;B6|
|Cereza|        A|
|Sandía|     B6;A|
|  Lima| D;A;B1;C|
|  Lima|   C;B1;A|
+------+---------+
only showing top 10 rows

>>> 
>>> df.select("color").distinct().show()
+--------+                                                                      
|   color|
+--------+
|  Morado|
|   Verde|
|    Rosa|
|Amarillo|
|    Rojo|
| Naranja|
+--------+

>>> df.select("nombre", "calorias_por_100g").orderBy(df["calorias_por_100g"].cast("float").desc()).show(10)
+-------+-----------------+                                                     
| nombre|calorias_por_100g|
+-------+-----------------+
|    Uva|            150.0|
|   Kiwi|            150.0|
| Cereza|            150.0|
|  Mango|            150.0|
|   Pera|            150.0|
|   Lima|            150.0|
|Ciruela|            150.0|
|   Lima|            150.0|
|Manzana|            150.0|
|   Kiwi|            150.0|
+-------+-----------------+
only showing top 10 rows
