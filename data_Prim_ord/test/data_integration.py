from pyspark.sql import SparkSession
import boto3


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
.config("spark.jars","./../../0jars/postgresql-42.7.3.jar") \
.config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
.master("local[*]") \
.getOrCreate()

try:
     # Create a bucket
    #s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',region_name='us-east-1')
    #bucket_name = 'my-local-bucket'                    
    #s3.create_bucket(Bucket=bucket_name)
    
    df = spark.read.csv("./sensoras.csv")
    ruta_salida = "s3a://my-local-bucket/data_reservas.csv"
    df=df.write.csv(ruta_salida, mode="overwrite")
        
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)

spark.stop()



# Formas de leer https://www.diegocalvo.es/leer-y-escribir-json-en-python/