from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split

# Inicializar una sesión Spark
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



# Ruta del archivo en el bucket
ruta_archivo = "s3a://my-local-bucket/data_battle_records.txt"

# Cargar el archivo como un DataFrame de Spark
df = spark.read.text(ruta_archivo)

# Separar cada línea por el carácter ':'
df = df.withColumn("Nombre", split(df["value"], ":").getItem(0))
df = df.withColumn("Pokemon_team", split(df["value"], ":").getItem(1))
df = df.withColumn("Battle_result", split(df["value"], ":").getItem(2))
df = df.withColumn("Event", split(df["value"], ":").getItem(3))
df = df.withColumn("Younger", split(df["value"], ":").getItem(4))

# Eliminar la columna 'value' original
df = df.drop("value")

# Guardar el DataFrame como un archivo CSV en el bucket S3
ruta_salida_csv = "s3a://my-local-bucket/data_battle_records.csv"
df.write.mode("overwrite").csv(ruta_salida_csv, header=True)

# Detener la sesión Spark
spark.stop()