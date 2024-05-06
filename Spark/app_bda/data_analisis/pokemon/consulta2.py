
import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_juego_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        #### FALTA EL NOMBRE ####
        cursor = connection.cursor()
        cursor.execute("""SELECT nombre 
                            FROM pokemon 
                            WHERE ataque = (SELECT MAX(ataque) FROM pokemon) """)
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            evento_ID = row[0]
            nombre = row[1]
            print("idEvento: " + str(evento_ID) + ", nombre: "+ nombre)

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" 3. ● ¿Qué Pokémon tiene el mayor ataque?")
print(select(connection))