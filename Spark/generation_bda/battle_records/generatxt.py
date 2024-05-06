import random
from faker import Faker
fake = Faker() # Crear una instancia de Faker

def menor_Edad():
    options = ['si', 'no']
    menor_edad = random.choice(options)
    return menor_edad

def generate_random_string(length=8):
    letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    return ''.join(random.choice(letters) for _ in range(length))

def generate_Pokemon(length=3):
    letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = 'POK_' + ''.join(random.choice(letters) for _ in range(length))
    return result

def generate_Evento(length=3):
    letters = '0123456789'
    result = 'Event_' + '/'.join(random.choice(letters) for _ in range(length))
    return result


def resultado():
    probabilidad = random.random()
    if probabilidad <= 0.5:   
        resultado = "Victoria"
    elif probabilidad <= 1:  
        resultado = "Derrota"
    return resultado

def create_text_file(filename, content):
    with open(filename, 'w') as file:
        file.write(content)
    print(f"File '{filename}' created successfully!")



# Generar un nombre falso
def generaNombre():
    fake_name = fake.name()
    return fake_name



data_to_create = {
    "Nombre_entrenador" : generaNombre(),
    "Equipo_pokemon" : [generate_Pokemon(), generate_Pokemon(), generate_Pokemon()],
    "Resultado_batalla" : resultado(),
    "Evento": generate_random_string(),
    "Menor_edad": menor_Edad()
}

conjunto=""
for i in range(1, 2001):
    #coach, pokemon_team, battle_result, event, younger
    linea = generaNombre() + ":" + generate_Pokemon()+ ", " + generate_Pokemon() + ", "  + generate_Pokemon() + ":" + resultado() + ":"  + generate_Evento() + ":" + menor_Edad() + "\n"
    
    conjunto = conjunto + linea

# print(conjunto)

#create_text_file('./../../1_data_bda/text/data_battle_records.txt', conjunto)
create_text_file('./../../1_data_bda/text/data_battle_records.txt', conjunto)
