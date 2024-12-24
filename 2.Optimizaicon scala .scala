import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Crear una sesión de Spark
val spark = SparkSession.builder
  .appName("MicroserviciosGestorDeTerminales")
  .enableHiveSupport()
  .getOrCreate()

// Definir el rango de fechas (si es necesario)
val fechaInicio = "2023-01-01"
val fechaFin = "2023-12-31"

// Cargar las tablas 'microservicios_versiones_t' y 'fecha_inicio_2_prueba'
val microserviciosDF = spark.sql("SELECT * FROM cld_bi_operacion_eng.microservicios_versiones_t")
val fechaInicioDF = spark.sql("SELECT fecha_inicio FROM cld_bi_operacion_eng.fecha_inicio_2_prueba")

// Obtener el valor de fecha_inicio
val fechaInicioValor = fechaInicioDF.collect()(0).getAs[java.sql.Timestamp]("fecha_inicio")

// Filtrar los datos por fecha_inicio
val filteredDF = microserviciosDF.filter(col("date_monitoring") >= fechaInicioValor && col("date_monitoring") <= fechaFin)

// Crear una ventana para la función row_number
val windowSpec = Window.partitionBy("terminal", "codigo_unico").orderBy(col("date_monitoring").desc)

// Agregar la columna 'ranker' usando row_number
val rankedDF = filteredDF.withColumn("ranker", row_number().over(windowSpec))

// Filtrar las filas donde ranker es 1
val resultDF = rankedDF.filter(col("ranker") === 1)
  .select("terminal", "codigo_unico", "version_contenedor", "date_monitoring", "fecha_carga")

// Agregar la columna 'serie'
val finalDF = resultDF.withColumn("serie", lit(""))

// Guardar el resultado en una nueva tabla
finalDF.write.mode("overwrite").saveAsTable("cld_bi_operacion_eng.microservicios_gestor_de_terminales")

// Detener la sesión de Spark
spark.stop()