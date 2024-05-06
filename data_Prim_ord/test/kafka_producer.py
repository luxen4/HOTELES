from time import sleep                      # Ejecutar con el botón derecho
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
import random  # Importa la librería random
                    
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
# producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))


# Function to read and display the contents of a text file
def read_text_file(filename):
    try:
        
        numLinea=1
        numReserva=0
        
        with open(filename, 'r') as file:
            reservas = []
            reserva_actual = {}
            
            for line in file:
                numReserva=numReserva +1
                line = line.strip()
                
                if line.startswith('*** Reserva'):
                   
                    if reserva_actual:
                        reservas.append(reserva_actual)
                        reserva_actual = {}
                    
                if not (line.startswith('*** Reserva') and (line.startswith(''))):
                    valores = line.split(':')
                    
                    if numLinea == 1:    id_cliente = valores[1]
                    elif numLinea == 2:  
                        fecha_llegada = valores[1]
                    elif numLinea == 3:  fecha_salida = valores[1]
                    elif numLinea == 4:  tipo_habitacion = valores[1]
                    elif numLinea == 5:  preferencias_comida = valores[1]
                    elif numLinea == 6:  
                        id_restaurante = valores[1]
                        
                        message = {
                        "id_reserva": "*** Reserva " + str(numReserva) + "***",
                        "timestamp": int(datetime.now().timestamp() * 1000),
                        "id_cliente": id_cliente,
                        "fecha_llegada": fecha_llegada,
                        "fecha_salida": fecha_salida,
                        "tipo_habitacion": tipo_habitacion,
                        "preferencias_comida": preferencias_comida,
                        "id_restaurante": id_restaurante
                        }
                        
                        print(message)
                        producer.send('info', value=message)
                        
                        numLinea = 6
                        sleep(1)
                        
                        
                    if numLinea==6:
                        numLinea=0
                    else:
                        numLinea = numLinea + 1

    except FileNotFoundError:
        print(f"File '{filename}' not found.")

filename='./reservas.txt'
read_text_file(filename)