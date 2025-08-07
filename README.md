# analisis_datos

Este proyecto muestra una serie de comandos básicos en PySpark para el análisis de datos a gran escala.

Para realizar esta prueba, se ha creado un clúster en **Amazon EMR** y se ha utilizado **Amazon S3** como sistema de almacenamiento, donde se subió un archivo `.csv` de gran tamaño (`frutas_100mb.csv`). Desde PySpark, se accede a este archivo directamente desde S3 para trabajar con él.

## Infraestructura utilizada

- **Amazon EMR**
  - Instancias tipo `m5.xlarge`
  - Roles: Primery, Core y Task
  - 16 GB de memoria por nodo

- **Amazon S3**
  - Archivo: `frutas_100mb.csv`
  - Ruta: `s3://paco-cubo/frutas_100mb.csv`

## Contenido del análisis

Algunas de las operaciones realizadas con PySpark:

- Carga de datos desde S3
- Revisión del esquema del dataset
- Conteo de valores nulos por columna
- Eliminación de filas con valores nulos (`df.na.drop()`)
- Filtros condicionales (por ejemplo: frutas tropicales, frutas con cierto peso o con vitamina A)
- Ordenamiento por calorías
- Visualización de valores únicos en columnas como `color`

## Ejemplo de comandos usados

```python
df = spark.read.csv('s3://paco-cubo/frutas_100mb.csv', header=True)
df = df.na.drop()
df.filter(df["es_tropical"] == "True").show()
df.filter(df["peso_promedio"].cast("float") > 500).select("nombre", "peso_promedio").show()
```

## Dataset

El dataset contiene campos como:

- nombre
- color
- sabor
- peso_promedio
- calorias_por_100g
- origen
- temporada
- es_tropical
- vitaminas

## Conclusión

Este pequeño proyecto sirve como ejemplo del uso de PySpark en un entorno distribuido para la exploración y limpieza de datos reales alojados en la nube con AWS.