from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
'''
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
'''

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
.config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
.master("local[*]") \
.getOrCreate()


try:

    from pyspark.sql.functions import from_json
    # Lee el archivo JSON como texto
    clientes_data = spark.read.json("clientes.json")
    clientes_data.show()
    
    # Filtra los registros corruptos
    registros_corruptos = clientes_data.filter(clientes_data["_corrupt_record"].isNotNull())

    # Cuenta el número de registros corruptos
    num_registros_corruptos = registros_corruptos.count()

    # Muestra el número de registros corruptos
    print("Número de registros corruptos:", num_registros_corruptos)

    # Convierte el texto JSON en una columna de estructura
    clientes_df = clientes_data.select(from_json("value", "array<struct<id_cliente:int,nombre:string,direccion:string,preferencias_alimenticias:string>>").alias("clientes"))

    # Explode la columna de estructura para obtener filas separadas
    clientes_df = clientes_df.selectExpr("explode(clientes) as cliente")

    # Selecciona los campos individuales del cliente
    clientes_df = clientes_df.select("cliente.id_cliente", "cliente.nombre", "cliente.direccion", "cliente.preferencias_alimenticias")

    # Muestra el contenido del DataFrame
    clientes_df.show()

    # Muestra el esquema del DataFrame
    clientes_df.printSchema()
                
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)

spark.stop()














'''
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',region_name='us-east-1')
        
def upload_file_to_s3(file_path, bucket_name, s3_key):
    try:
        with open(file_path, 'rb') as file:
            file_content = file.read()

        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content)

        print(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except ClientError as e:
        print(f"An error occurred: {e}")'''