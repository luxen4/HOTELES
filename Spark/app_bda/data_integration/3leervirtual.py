from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, when, col, mean
from datetime import datetime
from pyspark.sql.functions import col, year, month, dayofmonth , lit, concat, substring, to_date

# Crear la SparkSession
def sesion_Spark():
     
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
    
    return spark


def leerConSpark():
    spark = sesion_Spark()

    try:
        #df = spark.read.text("s3a://my-local-bucket/dataPokemon.json")
        df = spark.read.option("header", "true").csv("s3a://my-local-bucket/data_pokemon.csv/")
        df.show()
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)

leerConSpark()


def leerProcesarConSpark():
    
    spark = sesion_Spark()
    try:
        df_filtrado = spark.read.text("s3a://my-local-bucket/sales_data.csv")
        
        # Fechas
        columna_fecha = "date"
        df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%Y-%m-%d")})        # Nulos a fecha actual
        df_filtrado = df_filtrado.withColumn(columna_fecha, to_date(col(columna_fecha), "yyyy-M-d"))     # Castear a Date
        df_filtrado = df_filtrado.withColumn("Año", year(columna_fecha))
        df_filtrado = df_filtrado.withColumn("Mes", month(columna_fecha))
        df_filtrado = df_filtrado.withColumn("Dia", dayofmonth (columna_fecha))
        df_filtrado = df_filtrado.withColumn("Mes/Año", concat(month(columna_fecha), lit("-"), year(columna_fecha)))
            
        df_filtrado.show()
        
        df_filtrado \
        .write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
        .mode('overwrite') \
        .csv(path='s3a://my-local-bucket/sales_data_procesado.csv', sep=',')
        
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)
        
    
'''
# Leer con Boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test',aws_secret_access_key='test')        # Cliente de S3 para LocalStack

bucket_name = 'my-local-bucket'
csv_file_key = 'sales_datalimpio.csv'

response = s3.get_object(Bucket=bucket_name, Key=csv_file_key)
csv_data = response['Body'].read().decode('utf-8')
csv_data = csv_data.split('\n')
csv_reader = csv.reader(csv_data, delimiter=',')
next(csv_reader)                                      # Linea de encabezados. 
'''
