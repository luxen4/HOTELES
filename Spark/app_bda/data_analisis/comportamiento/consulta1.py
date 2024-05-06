
import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_juego_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT nombre, count(resultadobatalla) as num_batallas FROM comportamiento
                            GROUP BY nombre
                            ORDER BY COUNT(resultadobatalla) DESC
                            LIMIT 3;""")
        
        # SELECT evento_ID, nombre FROM evento WHERE fecha = '2000-11-1'
        
        rows = cursor.fetchall()

        for row in rows: 
            nombre = row[0]
            num_batallas= row[1]
            
            print("Nombre: " + nombre + ", nº de batallas ganadas: " + str(num_batallas) )

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" B-1. ● ¿Quién es el entrenador con más victorias registradas?")
print(select(connection))