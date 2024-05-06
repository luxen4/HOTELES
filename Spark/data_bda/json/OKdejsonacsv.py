from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructField, StructType
# Inicializar SparkSession


spark = SparkSession.builder \
.appName("SPARK S3") \
.config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
.config("spark.hadoop.fs.s3a.access.key", 'test') \
.config("spark.hadoop.fs.s3a.secret.key", 'test') \
.config("spark.sql.shuffle.partitions", "4") \
.config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
.config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
.master("spark://spark-master:7077") \
.getOrCreate()

try:
    # Inicializar una sesión Spark
    spark = SparkSession.builder \
        .appName("Procesamiento de Archivo JSON a CSV con Encabezados") \
        .getOrCreate()
    
    # Esquema para el DataFrame
    schema = StructType([
        StructField("Evento", StringType(), True),
        StructField("Fecha", StringType(), True),
        StructField("Descripción", StringType(), True)
    ])
    
    # Ruta del archivo JSON en el bucket S3
    ruta_entrada_json = "s3a://my-local-bucket/data_events.json"
    df = spark.read.json(ruta_entrada_json, schema=schema)
    ruta_salida_csv = "s3a://my-local-bucket/data_events.csv"
    df.write.mode("overwrite").option("header", "true").csv(ruta_salida_csv)
    df.show()

    # Esquema para el DataFrame
    schema = StructType([
        StructField("Nombre", StringType(), True),
        StructField("Tipo", StringType(), True),
        StructField("Estadisticas", StructType([
            StructField("HP", StringType(), True),
            StructField("Ataque", StringType(), True),
            StructField("Defensa", StringType(), True),
            StructField("Velocidad", StringType(), True),
        ]), True),
        StructField("Habilidades", StringType(), True),
        StructField("Evoluciones", StringType(), True)
    ])

    # Ruta del archivo JSON en el bucket S3
    ruta_entrada_json = "s3a://my-local-bucket/data_pokemon.json"

    # Leer el archivo JSON en un DataFrame de Spark con el esquema proporcionado
    df = spark.read.json(ruta_entrada_json, schema=schema)

    # Expandir las columnas del struct 'Estadisticas' a columnas individuales
    df = df.select("Nombre", "Tipo",
                col("Estadisticas.HP").alias("HP"),
                col("Estadisticas.Ataque").alias("Ataque"),
                col("Estadisticas.Defensa").alias("Defensa"),
                col("Estadisticas.Velocidad").alias("Velocidad"),
                "Habilidades", "Evoluciones")

    # Ruta de salida para el archivo CSV
    ruta_salida_csv = "s3a://my-local-bucket/data_pokemon.csv"

    # Escribir el DataFrame como un archivo CSV con encabezados
    df.write.mode("overwrite") \
        .option("header", "true").csv(ruta_salida_csv)
        
        
        
    
    df = spark.read.option("header", "true").csv("s3a://my-local-bucket/data_pokemon.csv")
    df.show()
    
    
    
    
        

    df.show()
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)

spark.stop()