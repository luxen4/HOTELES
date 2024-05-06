import psycopg2
from pyspark.sql import SparkSession
import csv

def createTable_Empleados():
    try:
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   
        # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   

        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS empleados (
                id_empleado SERIAL PRIMARY KEY,
                nombre VARCHAR (100),
                posicion VARCHAR (100),
                fecha_contratacion VARCHAR (100),
                id_hotel INT
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'EMPLEADO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


def insertarTable_Empleados(nombre, posicion, fecha_contratacion, id_hotel):
    
    connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO empleados (nombre, posicion, fecha_contratacion, id_hotel) VALUES (%s, %s, %s, %s);", 
                       (nombre, posicion, fecha_contratacion, id_hotel))
                
    connection.commit()     
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla Empleados.")
    

def leerCSV_Empleados(filename):
    
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        next_row = next(reader, None)  # Obtiene la siguiente fila
        for row in reader:
            print(row)
            id_empleado, nombre, posicion, fecha_contratacion, id_hotel = row
            insertarTable_Empleados(nombre, posicion, fecha_contratacion, id_hotel)
            



def createTable_Hoteles():
    try:
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   
        # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="PrimOrd_db", user="PrimOrd", password="bdaPrimOrd")   
        
        cursor = connection.cursor()
     
        create_table_query = """
            CREATE TABLE IF NOT EXISTS hoteles (
                id_hotel SERIAL PRIMARY KEY,
                nombre_hotel VARCHAR (100),
                direccion_hotel VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'HOTEL' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)
        

def insertarTable_Hoteles(id_hotel, nombre_hotel, direccion_hotel):
    
    connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO hoteles (id_hotel, nombre_hotel, direccion_hotel) VALUES (%s, %s, %s);", 
                       (id_hotel, nombre_hotel, direccion_hotel))
                
    connection.commit()     
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla Hoteles.")
           
        
def leerCSV_Hoteles(filename):
    
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Saltar la primera línea
        for row in reader:
            print(row)
            id_hotel, nombre_hotel, direccion_hotel = row
            insertarTable_Hoteles( id_hotel, nombre_hotel, direccion_hotel)


### DESDE UN JSON, O UN .txt, subir a postgres, neo4j, mysql, etc

createTable_Empleados()
filename='./empleados2.csv'
leerCSV_Empleados(filename)

createTable_Hoteles()
filename='./hoteles.csv'
leerCSV_Hoteles(filename)