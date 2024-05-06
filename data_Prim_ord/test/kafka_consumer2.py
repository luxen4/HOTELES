from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType,LongType
import boto3

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
    
df =spark  \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9093") \
  .option("subscribe", "info") \
  .option("failOnDataLoss",'false') \
  .load()
  

schema = StructType() \
    .add("id_reserva", StringType()) \
    .add("timestamp", LongType()) \
    .add("id_cliente", StringType()) \
    .add("fecha_llegada", StringType()) \
    .add("fecha_salida", StringType()) \
    .add("tipo_habitacion", StringType()) \
    .add("preferencias_comida", StringType()) \
    .add("id_restaurante", StringType()) 
    
    

# Convert value column to JSON and apply schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")
    

    

# Print schema of DataFrame for debugging
df.printSchema()

'''
query = df \
    .writeStream \
    .format("json") \
    .option("failOnDataLoss",'false') \
    .option("path", "./datoss") \
    .option("checkpointLocation", "./checkopoint") \
    .start()'''

'''
query = df \
    .writeStream \
    .format("csv") \
    .option("failOnDataLoss",'false') \
    .option("path", "s3a://my-local-bucket/data_reservas.csv") \
    .option("checkpointLocation", "checkpoint_dir") \
    .start()'''
    
'''
query = df.writeStream \
      .format("csv") \
      .option("sep", ",") \
      .option("header", "true") \
      .option("path", "s3a://my-local-bucket/data_reservas.csv") \
      .option("checkpointLocation", "./checkpoint_dir") \
      .start()'''
     

   
query = df.writeStream \
    .format("json") \
    .option("path", "s3a://my-local-bucket/data_reservas.json") \
    .option("checkpointLocation", "d") \
    .start()
    
    
 

# Wait for the termination of the querypython 
query.awaitTermination()