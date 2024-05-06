from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
.appName("Leer y procesar con Spark") \
.config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
.config("spark.hadoop.fs.s3a.access.key", 'test') \
.config("spark.hadoop.fs.s3a.secret.key", 'test') \
.config("spark.sql.shuffle.partitions", "4") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.master("local[*]") \
.getOrCreate()

# Define schema for JSON data
schema = StructType([
    StructField("id_cliente", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("preferencias_alimenticias", StringType(), True)
])

# Read JSON data from S3 into DataFrame
ruta_entrada_json = "s3a://my-local-bucket/clientes1.json/"
df = spark.read.json(ruta_entrada_json, schema=schema)

# Show DataFrame
df.show()

# Write DataFrame to CSV in S3
ruta_salida_csv = "s3a://my-local-bucket/clientes.csv"
df.write.mode("overwrite").option("header", "true").csv(ruta_salida_csv)

# Stop Spark session
spark.stop()