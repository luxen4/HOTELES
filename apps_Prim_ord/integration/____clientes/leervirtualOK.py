from pyspark.sql import SparkSession

# Crear la SparkSession
def sesion_Spark():
     
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
    .config("spark.jars","./../../../0jars/postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()
    
    return spark

def leerConSpark():
    spark = sesion_Spark()

    try:
        
        # df = spark.read.option("header", "true").csv("s3a://my-local-bucket/menus.csv")
        
        # IMPORTANTE LEER DE ESTA MANERA UN JSON
        df2 = spark.read.json("s3a://my-local-bucket/clientes2.json")
        df2.show()
        
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)

leerConSpark()

        
        
        
        
        
        
        
        
        
        
        
        
        
        
# https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/