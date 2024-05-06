from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType

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
     # Esquema para el DataFrame
    schema = StructType([
        StructField("id_cliente", IntegerType(), nullable=True, metadata={"description": "Id do cliente"}),
        StructField("nombre", StringType(), nullable=True, metadata={"description": "nombre do cliente"}),
        StructField("direccion", StringType(), nullable=True, metadata={"description": "Direccion do cliente"}),
        StructField("preferencias_alimenticias", StringType(), nullable=True, metadata={"description": "Preferencias do cliente"})
    ])
    
    # Lee el archivo JSON como texto
    ruta_entrada_json = "./clientes.json"
    df = spark.read.text(ruta_entrada_json)
    
    # Configura el ancho de la columna para mostrar el texto completo en la consola
    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 1000)
    df.show(truncate=False)
    
    
    from pyspark.sql.functions import from_json
    # Convierte el texto JSON en un DataFrame usando from_json
    df = df.select(from_json("value", schema).alias("data")).selectExpr("data.*")
    
    
    #df = spark.read.json(ruta_entrada_json, schema=schema)
    df.show(truncate=False)
    
    
    #ruta_salida_csv = "s3a://my-local-bucket/data_clientes.csv"
    #df.write.mode("overwrite").option("header", "true").csv(ruta_salida_csv)
    #df.show()
    
    #df = spark.read.option("header", "true").csv("s3a://my-local-bucket/data_clientes.csv")
       
    #df.show()
    
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)

spark.stop()



# https://panini.hashnode.dev/o-guia-definitivo-de-transformacoes-em-spark

# Para limpieza https://datawaybr.medium.com/como-sair-do-zero-no-delta-lake-em-apenas-uma-aula-d152688a4cc8

# https://panini.hashnode.dev/o-guia-definitivo-de-transformacoes-em-spark