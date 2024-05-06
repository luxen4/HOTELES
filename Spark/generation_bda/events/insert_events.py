import random
from datetime import datetime, timedelta

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
    doc = {"Evento": generate_random_string() ,"Fecha": generate_random_date() ,"Descripción": generate_random_string()  }
    collect.append(doc)
    
    
collection.insert_one({"eventos": collect})
print(collection)

# Realiza una consulta para encontrar todos los productos de tipo "pantalones"
consulta = { "Evento": "mYZ" }

# Ejecuta la consulta y obtén los resultados
resultados = collection.find(consulta)

# Imprime los resultados
print("encontrados:")
for producto in resultados:
    print(producto)