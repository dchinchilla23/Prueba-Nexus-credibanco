from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("MicroserviciosGestorDeTerminales") \
    .enableHiveSupport() \
    .getOrCreate()

# Definir el rango de fechas (si es necesario)
fecha_inicio = '2023-01-01'
fecha_fin = '2023-12-31'

# Cargar las tablas 'microservicios_versiones_t' y 'fecha_inicio_2_prueba'
microservicios_df = spark.sql("SELECT * FROM cld_bi_operacion_eng.microservicios_versiones_t")
fecha_inicio_df = spark.sql("SELECT fecha_inicio FROM cld_bi_operacion_eng.fecha_inicio_2_prueba")

# Obtener el valor de fecha_inicio
fecha_inicio_valor = fecha_inicio_df.collect()[0]['fecha_inicio']

# Filtrar los datos por fecha_inicio
filtered_df = microservicios_df.filter((col("date_monitoring") >= fecha_inicio_valor) & 
                                       (col("date_monitoring") <= fecha_fin))

# Crear una ventana para la función row_number
window_spec = Window.partitionBy("terminal", "codigo_unico").orderBy(col("date_monitoring").desc())

# Agregar la columna 'ranker' usando row_number
ranked_df = filtered_df.withColumn("ranker", row_number().over(window_spec))

# Filtrar las filas donde ranker es 1
result_df = ranked_df.filter(col("ranker") == 1).select("terminal", "codigo_unico", "version_contenedor", "date_monitoring", "fecha_carga")

# Agregar la columna 'serie'
result_df = result_df.withColumn("serie", lit(""))

# Guardar el resultado en una nueva tabla
result_df.write.mode("overwrite").saveAsTable("cld_bi_operacion_eng.microservicios_gestor_de_terminales")

# Detener la sesión de Spark
spark.stop()