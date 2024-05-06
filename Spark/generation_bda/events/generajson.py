import random, json
from datetime import datetime, timedelta

def create_json_file(filename, data):
    with open(filename, 'w') as file:
        for item in data:
            # Convertir la fecha a una cadena ISO antes de serializar el diccionario a JSON
            item['Fecha'] = item['Fecha'].isoformat()
            json.dump(item, file)
            file.write('\n')
    print(f"File '{filename}' created successfully!")



def generate_random_string(length=3):
    letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    return ''.join(random.choice(letters) for _ in range(length))


start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 12, 31)
def generate_random_date():
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

    
from pymongo import MongoClient

# Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
client = MongoClient()

db = client["pokemon_events_db"]
collection = db["eventos"]            # Accede a la colección "ropa"


collect=[]

for i in range(1, 2001):
    doc = {"evento": generate_random_string() ,"fecha": generate_random_date() ,"descripcion": generate_random_string()  }
    collect.append(doc)
    
    
create_json_file('./../../1_data_bda/mongodb/data_events.json', collect)
