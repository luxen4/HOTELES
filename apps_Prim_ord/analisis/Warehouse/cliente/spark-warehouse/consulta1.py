import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='primord_db', user='postgres', password='casa1234')
    
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT nombre, tipo_habitacion, preferencias_comida FROM cliente limit 100 """)
        rows = cursor.fetchall()

        for row in rows: 
            nombre, tipo_habitacion, preferencias_comida = row
            print("Nombre: " + nombre + ", tipo_habitacion: " + tipo_habitacion + ", Preferencias de comida: " + preferencias_comida )

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" Pregunta 1: ¿Qué clientes han hecho reservas y cuáles son sus preferencias de habitación y comida?")
print(select(connection))