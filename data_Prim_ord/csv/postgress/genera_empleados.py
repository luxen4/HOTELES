import csv
import random

# Nombre del archivo de empleados
archivo_empleados = "empleados.csv"

# Leer los datos existentes del archivo CSV
with open(archivo_empleados, mode='r') as file:
    reader = csv.DictReader(file)
    empleados = list(reader)

# Generar un hotel_id aleatorio para cada empleado y añadirlo como una nueva columna
for empleado in empleados:
    empleado['id_hotel'] = random.randint(1, 100)  # Suponiendo que los IDs de hotel van del 1 al 10

# Escribir los datos actualizados de vuelta al archivo CSV
campos = ['id_empleado', 'nombre', 'posicion', 'fecha_contratacion', 'id_hotel']

with open("empleados2.csv", mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=campos)
    writer.writeheader()
    writer.writerows(empleados)

print("Se ha añadido 'hotel_id' a cada empleado en el archivo", archivo_empleados)