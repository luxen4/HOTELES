import psycopg2
from pyspark.sql import SparkSession
        
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
    .master("local[*]") \
    .getOrCreate()
        

def createTable_Evento():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="warehouse_juego_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
                    CREATE TABLE IF NOT EXISTS evento (
                    evento_ID SERIAL PRIMARY KEY,
                    nombre VARCHAR (100),
                    fecha DATE,
                    descripcion VARCHAR (100)
                );
                """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'EVENTO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)

def insertarTable__Evento(nombre, fecha, descripcion):

    #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_juego_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO evento (nombre, fecha, descripcion) VALUES (%s, %s, %s);", 
                       (nombre, fecha, descripcion))
                
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla Eventos.")
    
    

def dataframe_Evento():
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
    
    try:

        #tiene que venir de pokemon del bucket, luego: meter el json al bucket
        bucket_name = 'my-local-bucket' 
        file_name='data_events.csv'
        
        df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df.show()
        
        for row in df.select("*").collect():
            print(row)
            nombre = row.Evento
            fecha = row.Fecha
            descripcion = row.Descripción
            # print(f"Ubicacion: {location}, revenue: {revenue}")
            
            insertarTable__Evento(nombre, fecha, descripcion)
   
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)
        
#createTable_comportamientoentrenador()
# createTable_Evento()
df = dataframe_Evento() 


