import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_juego_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT evento_ID,  nombre, fecha FROM evento 
                            WHERE fecha = (SELECT MAX(fecha) FROM evento) """)
        
        # SELECT evento_ID, nombre FROM evento WHERE fecha = '2000-11-1'
        
        rows = cursor.fetchall()

        for row in rows: 
            evento_ID = row[0]
            nombre = row[1]
            fecha = row[2]
            
            print("idEvento: " + str(evento_ID) + ", nombre: "+ nombre + ", fecha: "+ str(fecha))

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" C-2. ● ¿Cuál fue el evento más reciente?")
print(select(connection))