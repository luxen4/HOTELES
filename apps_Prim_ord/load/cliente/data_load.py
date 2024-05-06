import psycopg2
from pyspark.sql import SparkSession

# Pregunta 1: ¿Qué clientes han hecho reservas y cuáles son sus preferencias de habitación y comida?

def createTable_wcliente():
    try:
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   
        # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   
        
        cursor = connection.cursor()
        # nombre fecha_llegada tipo_habitacion, preferencias_alimenticias
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS wcliente (
                id_cliente SERIAL PRIMARY KEY,
                nombre VARCHAR (100),
                tipo_habitacion VARCHAR (100),
                preferencias_comida VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'WCLIENTE' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


def insertarTable_wcliente(id_cliente, nombre, tipo_habitacion, preferencias_comida):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO wcliente (id_cliente, nombre, tipo_habitacion, preferencias_comida) VALUES (%s, %s, %s, %s);", 
                       (id_cliente, nombre, tipo_habitacion, preferencias_comida))
                
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla Cliente.")
     
     
     
def dataframe_wcliente():
    
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

    try:
        bucket_name = 'my-local-bucket' 
        file_name = 'clientes.json'
        
        # IMPORTANTE LEER DE ESTA MANERA UN JSON
        df_clientes = spark.read.json(f"s3a://{bucket_name}/{file_name}")
        
        bucket_name = 'my-local-bucket' 
        file_name='data_reservas.csv'
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
         
        # Unir ambos DataFrames en función de la columna común
        df = df_clientes.join(df_reservas.select("id_cliente","tipo_habitacion","preferencias_comida"), "id_cliente", "left")
        df.show()
       
        # Agrupar"
        df = df[[col for col in df.columns if col != "preferencias_alimenticias"]]
        df = df[[col for col in df.columns if col != "direccion"]]
        
        df = df.dropDuplicates()    # Eliminar registros duplicados

        # Mostrar el DataFrame resultante
        df.show()
        
        # No tocar que es OK
        for row in df.select("*").collect():   
            print(row)
            id_cliente, nombre, tipo_habitacion, preferencias_comida = row
            print(f"id_cliente: {id_cliente},  Nombre: {nombre}, Tipo de Habitación: {tipo_habitacion}, Preferencias Comida: {preferencias_comida} ")
            
            insertarTable_wcliente(id_cliente, nombre, tipo_habitacion, preferencias_comida)
        
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)


createTable_wcliente()
dataframe_wcliente()



# Contar el número de registros en cada grupo
#df_grouped_count = df_grouped.count()
#df_grouped_count.show()


# https://blog.revolve.team/2023/05/02/data-files-from-s3-in-local-pyspark-environment/

### Clientes
# Pregunta 1: ¿Qué clientes han hecho reservas y cuáles son sus preferencias de
# habitación y comida?
