import json
import random
import string

def generate_random_string(length=5):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def tipo():
    tipos = ["Planta", "Agua", "Fuego", "Eléctrico", "Hielo", "Tierra", "Roca", "Veneno", "Volador"]
    return random.choice(tipos)

def habilidad():
    habilidades = ["Menta", "Espesura", "Clorofila", "Torrente", "Absorbe Agua", "Electricidad Estática"]
    return random.choice(habilidades)

data = []
for i in range(1, 2001):
    data_to_create = {
        "Nombre": generate_random_string(),
        "Tipo": tipo(),
        "Estadisticas": {
            "HP": random.randint(1, 100),
            "Ataque": random.randint(1, 100),
            "Defensa": random.randint(1, 100),
            "Velocidad": random.randint(1, 100),
        },
        "Habilidades": [habilidad(), habilidad()],
        "Evoluciones": [
            {"Nombre": generate_random_string(), "Tipo": tipo()},
            {"Nombre": generate_random_string(), "Tipo": tipo()},
        ]
    }
    data.append(data_to_create)

# Write JSON data to file
with open('./../../1_data_bda/json/data_pokemon.json', 'w') as json_file:
    for item in data:
        json.dump(item, json_file)
        json_file.write('\n')

print("JSON generado sin lista")