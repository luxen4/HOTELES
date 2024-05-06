import psycopg2
from pyspark.sql import SparkSession

def createTable_Comportamiento():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_juego_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS comportamiento (
                comportamiento_ID SERIAL PRIMARY KEY,
                nombre VARCHAR (100),
                equipo VARCHAR (100),
                resultadobatalla VARCHAR (100),
                younger VARCHAR (5)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'COMPORTAMIENTO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)

def insertarTable_Comportamiento(Nombre, Pokemon_team, Battle_result, Event, Younger):
    
    #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_juego_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO comportamiento (nombre, equipo, resultadobatalla, younger) VALUES (%s, %s, %s, %s);", 
                       (Nombre, Pokemon_team, Battle_result, Younger))
                
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla Comportamiento.")
   

def dataframe_Coportamiento():
    
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
        bucket_name = 'my-local-bucket' 
        file_name='data_battle_records.csv'
        
        df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df.show()
        
        for row in df.select("*").collect():
            print (row)
            Nombre, Pokemon_team, Battle_result, Event, Younger = row
            # print(f"Ubicacion: {location}, revenue: {revenue}")
            
            insertarTable_Comportamiento(Nombre, Pokemon_team, Battle_result, Event, Younger)
   
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)
   
#createTable_comportamientoentrenador()
#createTable_Comportamiento()
df = dataframe_Coportamiento() 
