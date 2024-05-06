from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
import pyspark.sql.functions as F


def sesionSpark():
    spark = SparkSession.builder \
        .appName("Leer y procesar con Spark") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
        .config("spark.hadoop.fs.s3a.access.key", 'test') \
        .config("spark.hadoop.fs.s3a.secret.key", 'test') \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
        .config("spark.jars","./hadoop-aws-3.4.0.jar") \
        .master("local[*]") \
        .getOrCreate()
    return spark

# Crear una sesión de Spark
spark = sesionSpark()

# Configuración de Kafka
kafka_params = {"kafka.bootstrap.servers": "kafka:9093", "subscribe": "info", "startingOffsets": "earliest"}
kafka_stream = spark.readStream.format("kafka").options(**kafka_params).load()

# Definir el esquema para los datos de Kafka
schema = StructType([
    StructField("id_reserva", StringType()),
    StructField("timestamp", LongType()),
    StructField("id_cliente", StringType()),
    StructField("fecha_llegada", StringType()),
    StructField("fecha_salida", StringType()),
    StructField("tipo_habitacion", StringType()),
    StructField("preferencias_comida", StringType()),
    StructField("id_restaurante", StringType())
])

# Generar una columna por cada clave
parsed_df = kafka_stream.select(from_json(col("value").cast("string"), schema).alias("input"))

# Seleccionar las columnas relevantes
selected_df = parsed_df.select("input.id_reserva", "input.timestamp", "input.id_cliente", "input.fecha_llegada", "input.fecha_salida", "input.tipo_habitacion", "input.preferencias_comida", "input.id_restaurante")



# Acción para cada lote de datos: Escribir el DataFrame resultante como un archivo CSV
def process_batch(batch_df, batch_id):
    #ruta_salida_csv = "s3a://my-local-bucket/data_reservas.csv"
    #batch_df.write.csv(ruta_salida_csv, mode="append", header=True)
    
    
    batch_df = batch_df.toPandas()
    #batch_df.to_csv("./sensors.csv")
    #batch_df.to_csv("./sensors.csv", index=False)
    
    batch_df.to_csv("./data_reservas.csv", index=False, header=["id", "timestamp", "id_cliente", "fecha_llegada", "fecha_salida", "tipo_habitacion", "preferencias_comida", "id_restaurante"])
    



# Iniciar la consulta de streaming y aplicar la función `process_batch` en cada lote
query = selected_df.writeStream.foreachBatch(process_batch).start()






# Probar
query = selected_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "s3a://my-local-bucket/data_reservas.csv") \
    .option("checkpointLocation", "/opt/spark-data/checkopoint")\
    .option("header", "true")\
    .start()




# Esperar hasta que la consulta de streaming termine (ya no haya más datos)
query.awaitTermination()

# Después de que la consulta termine, cargar el CSV en un DataFrame de Spark y guardarlo en S3
df = spark.read.csv("reservas.csv", header=True)
df.write.csv("s3a://my-local-bucket/data_reservas.csv", mode="overwrite", header=True)