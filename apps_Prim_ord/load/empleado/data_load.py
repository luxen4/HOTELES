import psycopg2
from pyspark.sql import SparkSession

def createTable_Empleados():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234") 
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   
        #connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   
        
        cursor = connection.cursor()
        
        # nombre fecha_llegada tipo_habitacion, preferencias_alimenticias
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS warehouseempleado (
                empleado_id SERIAL PRIMARY KEY,
                nombre VARCHAR (100),
                posicion VARCHAR (100),
                fecha_contratacion DATE,
                nombre_hotel VARCHAR (100)
            );
        """
        
        
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'WAREHOUSE_EMPLEADO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


def insertarTable_Empleados(nombre, posicion, fecha_contratacion, nombre_hotel):
    
    # connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
    
    # EXAMEN
    #connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   
        
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO warehouseempleado (nombre, posicion, fecha_contratacion, nombre_hotel) VALUES (%s, %s, %s, %s);", 
                       (nombre, posicion, fecha_contratacion, nombre_hotel))
                
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla WAREHOUSEEMPLEADO.")
     

def dataframe_Empleado():
    
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
        file_empleados='data_empleados.csv'
        df_empleados = spark.read.csv(f"s3a://{bucket_name}/{file_empleados}", header=True, inferSchema=True)
        df_empleados.show()
        
        
        bucket_name = 'my-local-bucket' 
        file_name='data_hoteles.csv'
        
        df_restaurantes = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        
        # Unir ambos DataFrames en función de la columna común
        df = df_empleados.join(df_restaurantes.select("id_hotel","nombre_hotel"), "id_hotel", "left")
        
        # Crear un nuevo DataFrame sin la columna "demographics"
        df = df[[col for col in df.columns if col != "id_hotel"]]
        df = df[[col for col in df.columns if col != "id_empleado"]]
        df.show()
        
        # No tocar que es OK
        for row in df.select("*").collect():   
            print(row)         
            nombre, posicion, fecha_contratacion, nombre_hotel= row
            #print(f"Ubicacion: {location}, revenue: {revenue}")
            
            insertarTable_Empleados(nombre, posicion, fecha_contratacion, nombre_hotel)
   
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)



createTable_Empleados()
dataframe_Empleado()

### Empleado
# Pregunta 3: ¿Quiénes son los empleados que trabajan en cada restaurante, junto
# con sus cargos y fechas de contratación?

# nombre_empleado  cargo_empleado  nombre_restaurante fecha_contratacion 
