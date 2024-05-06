import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='primord_db', user='postgres', password='casa1234')
    
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT nombre, posicion, fecha_contratacion, nombre_hotel FROM warehouseempleado """)
        rows = cursor.fetchall()

        for row in rows: 
            nombre, posicion, fecha_contratacion, nombre_hotel = row
            print("Nombre: " + nombre + ", Posición: " + posicion + ", Fecha de Contratación: " + str(fecha_contratacion) + " Nombre del Hotel: " + nombre_hotel )

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" Pregunta 1: ¿Qué clientes han hecho reservas y cuáles son sus preferencias de habitación y comida?")
print(select(connection))