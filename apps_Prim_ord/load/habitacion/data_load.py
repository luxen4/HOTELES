import psycopg2
from pyspark.sql import SparkSession

def createTable_Habitaciones():
    try:
        # EXAMEN
        #connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="PrimOrd_db", user="postgres", password="casa1234")   
        
        cursor = connection.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS habitacion (
                habitacion_id SERIAL PRIMARY KEY,
                id_reserva INTEGER,
                tipo_habitacion VARCHAR (100),
                categoria VARCHAR (100)
                tarifa_nocturna DECIMAL (7,3)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'HABITACIÓN' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


def insertarTable_Habitaciones(id_reserva, tipo_habitacion, categoria, tarifa_nocturna):

    connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO habitaciones (id_reserva, tipo_habitacion, categoria, tarifa_nocturna) VALUES (%s, %s, %s);", 
                       (id_reserva, tipo_habitacion, categoria, tarifa_nocturna))
                
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla Habitacion.")
     
# OK
def dataframe_Habitacion():
    # .JSON DE CLIENTES Y TXT DE RESERVAS
    
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
        file_name='data_reservas.csv'
        
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_reservas.show()
        
        
        bucket_name = 'my-local-bucket' 
        file_name='data_habitaciones.csv'
        
        df_habitaciones = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_habitaciones.show()
        
        
        # Unir ambos DataFrames en función de la columna común
        df = df_reservas.join(df_reservas.select("id_habitacion","categoria","tarifa_nocturna"), "id_habitacion", "left")
        df.show()
        
        
        
        # Crear un nuevo DataFrame sin la columna "demographics"
        df = df[[col for col in df.columns if col != "Tipo"]]
        df = df[[col for col in df.columns if col != "Defensa"]]
        df = df[[col for col in df.columns if col != "Velocidad"]]
        df = df[[col for col in df.columns if col != "Evoluciones"]]
        
        # No tocar que es OK
        for row in df.select("*").collect():   
            print(row)         
            id_reserva, tipo_habitacion, categoria, tarifa_nocturna = row
            # print(f"Ubicacion: {location}, revenue: {revenue}")
            
            
            insertarTable_Habitaciones(id_reserva, tipo_habitacion, categoria, tarifa_nocturna)
       
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)
        
        
# createTable_Habitaciones()
        
### Habitaciones
# Pregunta 2: ¿Qué habitaciones hay reservadas para cada reserva, y cuáles son sus
# respectivas categorías y tarifas nocturnas?

# txt tipo_habitacion(TXT)   categorias de(HABITACION)       tarifa_nocturna
