import psycopg2
from pyspark.sql import SparkSession

def createTable_Reservas():
    try:

        # EXAMEN
        connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   
        
        cursor = connection.cursor()
        
        # nombre fecha_llegada tipo_habitacion, preferencias_alimenticias
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS reserva (
                reserva_id SERIAL PRIMARY KEY,
                cliente_id integer,
                categoria_habitacion VARCHAR (100),
                preferencias_alimenticias VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'RESERVA' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


def insertarTable_Reserva(reserva_id, cliente_id, categoria_habitacion, preferencias_alimenticias):
    
    # EXAMEN
    connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   
        
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO reserva (reserva_id, cliente_id, categoria_habitacion, preferencias_alimenticias) VALUES (%s, %s, %s, %s);", 
                       (reserva_id, cliente_id, categoria_habitacion, preferencias_alimenticias))
                
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla RESERVA.")
     

def dataframe_Reservas():
    
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
        file_name='data_clientes.csv'
        
        df_clientes = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_clientes.show()
        
        
         # Unir ambos DataFrames en función de la columna común
        df = df_reservas.join(df_clientes.select("id_cliente","nombre","preferencias_alimenticias"), "id_cliente", "left")
        df.show()
        
        
        bucket_name = 'my-local-bucket' 
        file_name='data_habitaciones.csv'
        
        df_habitaciones = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_habitaciones.show()
        
        
         # Unir ambos DataFrames en función de la columna común
        df = df.join(df_habitaciones.select("id_cliente","categoria"), "id_habitacion", "left")
        df.show()
        
        
        
        
        # Crear un nuevo DataFrame sin la columna "demographics"
        df = df[[col for col in df.columns if col != "Tipo"]]
        df = df[[col for col in df.columns if col != "Defensa"]]
        df = df[[col for col in df.columns if col != "Velocidad"]]
        df = df[[col for col in df.columns if col != "Evoluciones"]]
        
        # No tocar que es OK
        for row in df.select("*").collect():   
            print(row)         
            reserva_id, cliente_id, categoria, preferencias_alimenticias = row
            # print(f"Ubicacion: {location}, revenue: {revenue}")
            
            
            insertarTable_Reservas(reserva_id, cliente_id, categoria, preferencias_alimenticias)
        
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)

createTable_Reservas()
dataframe_Reservas()

### Reserva
# Pregunta 4: ¿Cuántas reservas se hicieron para cada categoría de habitación, y
# cuáles son las correspondientes preferencias de comida de los clientes?

# categoria habitaciones.csv ()    preferencias_alimenticias