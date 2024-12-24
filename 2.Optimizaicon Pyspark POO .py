from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window

class MicroserviciosGestorDeTerminales:
    def __init__(self, app_name, hive_support=True):
        self.spark = SparkSession.builder.appName(app_name)
        if hive_support:
            self.spark = self.spark.enableHiveSupport()
        self.spark = self.spark.getOrCreate()

    def load_data(self):
        self.microservicios_df = self.spark.sql("SELECT * FROM cld_bi_operacion_eng.microservicios_versiones_t")
        self.fecha_inicio_df = self.spark.sql("SELECT fecha_inicio FROM cld_bi_operacion_eng.fecha_inicio_2_prueba")

    def filter_data(self, fecha_inicio, fecha_fin):
        fecha_inicio_valor = self.fecha_inicio_df.collect()[0]['fecha_inicio']
        self.filtered_df = self.microservicios_df.filter((col("date_monitoring") >= fecha_inicio_valor) & 
                                                         (col("date_monitoring") <= fecha_fin))

    def process_data(self):
        window_spec = Window.partitionBy("terminal", "codigo_unico").orderBy(col("date_monitoring").desc())
        self.ranked_df = self.filtered_df.withColumn("ranker", row_number().over(window_spec))
        self.result_df = self.ranked_df.filter(col("ranker") == 1).select("terminal", "codigo_unico", "version_contenedor", "date_monitoring", "fecha_carga")
        self.result_df = self.result_df.withColumn("serie", lit(""))

    def save_data(self, table_name):
        self.result_df.write.mode("overwrite").saveAsTable(table_name)

    def stop_spark(self):
        self.spark.stop()

if __name__ == "__main__":
    app = MicroserviciosGestorDeTerminales(app_name="MicroserviciosGestorDeTerminales")
    app.load_data()
    app.filter_data(fecha_inicio='2023-01-01', fecha_fin='2023-12-31')
    app.process_data()
    app.save_data(table_name="cld_bi_operacion_eng.microservicios_gestor_de_terminales")
    app.stop_spark()