from pyspark.sql import SparkSession
'''import boto3

# Create a bucket
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',region_name='us-east-1')
bucket_name = 'my-local-bucket'                     
s3.create_bucket(Bucket=bucket_name)'''

def sesionSpark():
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
    .config("spark.jars","./postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()
    
    return spark

# Meter directamente los archivos a S3
try:
    spark=sesionSpark()
    
    
    # csv
    df = spark.read.csv("./../../data_Prim_ord/csv/habitaciones.csv")
    ruta_salida = "s3a://my-local-bucket/habitaciones.csv"
    df=df.write.csv(ruta_salida, mode="overwrite")
    
    
    df = spark.read.csv("./../../data_Prim_ord/csv/menus.csv")
    ruta_salida = "s3a://my-local-bucket/menus.csv"
    df=df.write.csv(ruta_salida, mode="overwrite")
    
    df = spark.read.csv("./../../data_Prim_ord/csv/hoteles.csv")
    ruta_salida = "s3a://my-local-bucket/hoteles.csv"
    df=df.write.csv(ruta_salida, mode="overwrite")
    
    
    # json      # IMPORTANTE ESCRIBIR DE ESTA MANERA EL JSON #
    df = spark.read.option("multiline", "true").json("./../../data_Prim_ord/json/clientes.json")
    ruta_salida = "s3a://my-local-bucket/clientes.json"
    df.write.option("multiline", "true").json(ruta_salida, mode="overwrite")
    df.show()
    
    #df = spark.read.json("./../../data_Prim_ord/json/restaurantes.json")
    df = spark.read.option("multiline", "true").json("./../../data_Prim_ord/json/restaurantes.json")
    ruta_salida = "s3a://my-local-bucket/restaurantes.json"
    df.write.option("multiline", "true").json(ruta_salida, mode="overwrite")
    
    
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)





# Formas de leer https://www.diegocalvo.es/leer-y-escribir-json-en-python/