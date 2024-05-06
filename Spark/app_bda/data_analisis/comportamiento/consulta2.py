
import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_juego_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT equipo, count(resultadobatalla) as num_resultados FROM COMPORTAMIENTO
                                WHERE resultadobatalla = 'Victoria'
                                GROUP BY equipo
                                ORDER BY COUNT(*) ASC
                                LIMIT 4;""")
                                

        
        # SELECT evento_ID, nombre FROM evento WHERE fecha = '2000-11-1'
        
        rows = cursor.fetchall()

        for row in rows: 
            nombre = row[0]
            num_resultados = row[1]
            
            print("Equipo: " + nombre + "\nnº resultados: " + str(num_resultados) )

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" B-2. ● ¿Cuál es el equipo de Pokémon más utilizado por los entrenadores ganadores?")
print(select(connection))