import psycopg2
from pyspark.sql import SparkSession

########################################## Pokemon
# OK
def createTable_Pokemon():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234") 
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_juego_db", user="postgres", password="casa1234")   
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS pokemon (
                pokemon_id SERIAL PRIMARY KEY,
                nombre VARCHAR (100),
                hp INTEGER,
                ataque INTEGER,
                habilidad VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'POKEMON' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


# OK
def insertarTable_Pokemon(nombre, hp, ataque, habilidad):
    
    #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_juego_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO pokemon (nombre, hp, ataque, habilidad) VALUES (%s, %s, %s, %s);", 
                       (nombre, hp, ataque, habilidad))
                
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla Pokemon.")
     
# OK
def dataframe_Pokemon():
    
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
        file_name='data_pokemon.csv'
        
        df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df.show()
        
        # Crear un nuevo DataFrame sin la columna "demographics"
        df = df[[col for col in df.columns if col != "Tipo"]]
        df = df[[col for col in df.columns if col != "Defensa"]]
        df = df[[col for col in df.columns if col != "Velocidad"]]
        df = df[[col for col in df.columns if col != "Evoluciones"]]
        
        # No tocar que es OK
        for row in df.select("*").collect():   
            print(row)         
            Nombre, HP, Ataque, Habilidades = row
            # print(f"Ubicacion: {location}, revenue: {revenue}")
            
            insertarTable_Pokemon(Nombre, HP, Ataque, Habilidades)
   
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)
        
   
### TABLAS CREADAS OK ###
createTable_Pokemon()
df = dataframe_Pokemon() 
