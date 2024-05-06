import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_juego_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT COUNT(evento_ID) AS total FROM evento""")
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            print(row)
            total = row[0]
            linea="Total eventos: " + str(total)
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" C-1. ● ¿Cuántos eventos especiales se han registrado en la base de datos?")
print(select(connection))