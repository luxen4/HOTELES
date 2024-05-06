# ● ¿Qué descripción tiene el evento con más batallas?
 
 
import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_juego_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT habilidad FROM pokemon
                            GROUP BY habilidad
                            ORDER BY COUNT(habilidad) DESC
                            LIMIT 2;""")
        
        rows = cursor.fetchall()

        for row in rows: 
            descripcion = row[0]
            print("Descripción: " + str(descripcion) )

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)

print(" C-3. ● ¿Cuáles son las habilidades más comunes entre los Pokémon?")
print(select(connection))