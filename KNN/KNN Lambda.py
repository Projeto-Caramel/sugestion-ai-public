import json
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId
import time


inicio = time.time()

client = MongoClient({MongoDB}, serverSelectionTimeoutMS=50000)
# Ping nktstudios.com and get the status code
try:
    # Verifica se o servidor está acessível
    client.server_info()
    print("Conectado com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ao servidor MongoDB: {e}")

mongo_time = time.time()

db = client["sugestion"]
collection = db["algorithm-data"]

data = collection.find()

df = pd.DataFrame(list(data))

if '_id' in df.columns:
    df = df.drop(columns=['_id'])

lista = [n for n in range(1, len(df) + 1)]
df['idUser'] = lista
df = df.set_index('idUser')

users = df.iloc[:, :7]
pets = df.iloc[:, 7:16]
database = users.join(pets)

db_users = client['users-manegment']
collection_users = db_users['adopters-data']

db_pets = client['pets-managment']
collection_pets = db_pets['pets-data']

def retorna_vetor_usuario_db(user_id):
    usuario = collection_users.find_one({'_id': ObjectId(user_id)}, {"_id": 0,  "Birthday": 1,"ResidencyType": 1, "Lifestyle": 1,
                                        "PetExperience": 1, "HasChildren": 1, "FinancialSituation": 1, "FreeTime": 1})
    hoje = datetime.now()
    data_nascimento = datetime(usuario["Birthday"]["Year"], usuario["Birthday"]["Month"], usuario["Birthday"]["Day"])
    idade = hoje.year - data_nascimento.year - ((hoje.month, hoje.day) < (data_nascimento.month, data_nascimento.day))
    valor = 0
    if idade <= 25:
        valor = 0
    elif 26 <= idade <= 35:
        valor = 1
    elif 36 <= idade <= 45:
        valor = 2
    elif 46 <= idade <= 55:
        valor = 3
    elif 56 <= idade <= 65:
        valor = 7
    elif idade > 65:
        valor = 7

    vetor_usuario = []
    vetor_usuario.append(valor)

    if usuario["PetExperience"] == 2:
        vetor_usuario.append(0)
    else:
        vetor_usuario.append(1) 

    if usuario["ResidencyType"] == 1:
        vetor_usuario.append(0)
    elif usuario["ResidencyType"] == 2:
        vetor_usuario.append(2) 
    elif usuario["ResidencyType"] == 3:
        vetor_usuario.append(4) 

    if usuario["HasChildren"] == 2:
        vetor_usuario.append(0)
    else:
        vetor_usuario.append(3) 
    
    if usuario["FinancialSituation"] == 1:
        vetor_usuario.append(4)
    elif usuario["FinancialSituation"] == 2:
        vetor_usuario.append(2)
    else:
        vetor_usuario.append(0)
    
    if usuario["Lifestyle"] == 1:
        vetor_usuario.append(1.5)
    elif usuario["Lifestyle"] == 2:
        vetor_usuario.append(1)
    elif usuario["Lifestyle"] == 3:
        vetor_usuario.append(0.5)
    else:
        vetor_usuario.append(0)
    
    if usuario["FreeTime"] == 4:
        vetor_usuario.append(6)
    elif usuario["FreeTime"] == 3:
        vetor_usuario.append(4)
    elif usuario["FreeTime"] == 2:
        vetor_usuario.append(2)
    else:
        vetor_usuario.append(0)

    return np.array(vetor_usuario)

def retorna_vetor_pet_db(pet):    
    hoje = datetime.now()
    data_str = str(pet["Info"]["BirthDate"])
    data_nascimento = datetime.strptime(data_str.split(" ")[0], "%Y-%m-%d").date()
    idade = hoje.year - data_nascimento.year - ((hoje.month, hoje.day) < (data_nascimento.month, data_nascimento.day))
    valor = 0
    if idade < 1:
        valor = 0
    elif 1 <= idade <= 3:
        valor = 1
    elif 4 <= idade <= 7:
        valor = 2
    elif 8 <= idade:
        valor = 3
    valor = 1
    pet = pet["Caracteristics"]
    vetor_pet = []
    vetor_pet.append(valor) # idade
    vetor_pet.append(0) # energy level

    if pet["Size"] == 1:
        vetor_pet.append(0)
    elif pet["Size"] == 2:
        vetor_pet.append(0.5) 
    elif pet["Size"] == 3:
        vetor_pet.append(1) 
    
    if pet["StimulusLevel"] == 1:
        vetor_pet.append(0)
    elif pet["StimulusLevel"] == 2:
        vetor_pet.append(2.5) 
    elif pet["StimulusLevel"] == 3:
        vetor_pet.append(5) 

    if pet["Temperament"] == 3:
        vetor_pet.append(2)
    elif pet["Temperament"] == 2:
        vetor_pet.append(1) 
    else:
        vetor_pet.append(0) 

    if pet["ChildLove"] == 1:
        vetor_pet.append(0)
    elif pet["ChildLove"] == 2:
        vetor_pet.append(4) 
    elif pet["ChildLove"] == 3:
        vetor_pet.append(8) 
    
    if pet["AnimalsSocialization"] == 1:
        vetor_pet.append(0)
    elif pet["AnimalsSocialization"] == 2:
        vetor_pet.append(2) 
    elif pet["AnimalsSocialization"] == 3:
        vetor_pet.append(4)

    if pet["SpecialNeeds"] == 1:
        vetor_pet.append(0)
    elif pet["SpecialNeeds"] == 2:
        vetor_pet.append(1.5) 
    elif pet["SpecialNeeds"] == 3:
        vetor_pet.append(2)

    if pet["Shedding"] == 1:
        vetor_pet.append(0.2)
    elif pet["Shedding"] == 2:
        vetor_pet.append(0.1) 
    elif pet["Shedding"] == 3:
        vetor_pet.append(0)

    return np.array(vetor_pet)

def distancia_entre_vetores(a, b):
    return np.linalg.norm(a - b)

def retorna_vetor_usuario(user_id):
    data = database
    vetor = []
    user = data.query('idUser == %d' % user_id)
    for value in user.columns[:7]: ## percorre as 7 primeiras colunas
        vetor.append(user[value].values[0])
    return np.array(vetor)

def retorna_vetor_pet(user_id):
    data = database
    vetor = []
    user = data.query('idUser == %d' % user_id)
    for value in user.columns[7:]: ## percorre as 7 primeiras colunas
        vetor.append(user[value].values[0])
    return np.array(vetor)

def distancia_entre_usuarios(user_id1, user_id2):
    distancia = distancia_entre_vetores(user_id1, user_id2)
    return [user_id1, user_id2, distancia]

def distancia_entre_pets(user_id1, user_id2):
    distancia = distancia_entre_vetores(user_id1, user_id2)
    return [user_id1, user_id2, distancia]

def distancia_de_todos_user(user_id, numero_de_usuarios_a_analisar = None):
    data = database
    todos_os_usuarios = data.index.unique()
    if numero_de_usuarios_a_analisar:
        todos_os_usuarios = todos_os_usuarios[:numero_de_usuarios_a_analisar]
    distancias = []
    for usuario in todos_os_usuarios:
        distancias.append([user_id, usuario, 
                        distancia_entre_usuarios(user_id, retorna_vetor_usuario(usuario))[2]])
    distancias = pd.DataFrame(distancias, columns = ['voce', 'outra_pessoa', 'distancia'])
    return(distancias)

def distancia_de_todos_pet(pet_id, numero_de_usuarios_a_analisar = None):
    data = database
    todos_os_pets = data.index.unique()
    if numero_de_usuarios_a_analisar:
        data = data[:numero_de_usuarios_a_analisar]
    distancias = []
    for pet in todos_os_pets:
        distancias.append([pet_id, pet, 
                        distancia_entre_pets(retorna_vetor_pet(pet_id), retorna_vetor_pet(pet))[2]])
    distancias = pd.DataFrame(distancias, columns = ['seu pet', 'outro_pet', 'distancia'])
    return(distancias)
    
def distancia_de_todos_pet_db(pet_ideal, numero_de_usuarios_a_analisar = None):
    # data = df_pets
    data = collection_pets.find({}, {"_id": 1, "PartnerId": 1, "Caracteristics": 1, "Info": 1})
    if numero_de_usuarios_a_analisar:
        data = data[:numero_de_usuarios_a_analisar]
    distancias = []
    for pet in data:
        pet_vetor = retorna_vetor_pet_db(pet)
        if len(pet_vetor) == 9:
            distancias.append([pet, distancia_entre_pets(pet_ideal, pet_vetor)[2]])
    distancias = pd.DataFrame(distancias, columns = ['outro_pet', 'distancia'])
    return(distancias)        

def knn_user(user_id, k_mais_proximos=10, numero_de_usuarios_a_analisar = None):
    distancias = distancia_de_todos_user(user_id, numero_de_usuarios_a_analisar = numero_de_usuarios_a_analisar)
    distancias = distancias.sort_values("distancia")
    distancias = distancias.set_index("outra_pessoa")
    return distancias.head(k_mais_proximos)

def knn_pet(user_id, k_mais_proximos=2, numero_de_usuarios_a_analisar = None):
    distancias = distancia_de_todos_pet(user_id, numero_de_usuarios_a_analisar = numero_de_usuarios_a_analisar)
    distancias = distancias.sort_values("distancia")
    distancias = distancias.set_index("outro_pet")
    distancias = distancias.drop(user_id)    
    return distancias.head(k_mais_proximos)

def return_validation_array(users):
    D = [0 for n in users[0]]
    for user in users:
        for n in range(len(user)):
            D[n] += user[n]/len(users)        
    return D

def knn_db(pet_ideal, k_mais_proximos=5):
    pets = distancia_de_todos_pet_db(pet_ideal)
    pets = pets.sort_values("distancia")
    pets = pets.set_index("outro_pet")
    return pets.head(k_mais_proximos)

def return_recommended_pets(user_id, k_users = 10, k_pets = 2):
    users_list = knn_user(user_id, k_mais_proximos=k_users).index
    recommended_pets = []
    for user in users_list:
        knn_pets = knn_pet(user, k_mais_proximos=k_pets).index.values
        for pet in knn_pets:
            recommended_pets.append(pet)
    try:
        recommended_pets.remove(user_id)
        return recommended_pets
    except:
        return recommended_pets
    
try:
    # user = list(event["key1"])
    # user_id = event.get('queryStringParameters', {}).get('key1')
    user_id = "673393668a30ebdd690e02f0"
    user = retorna_vetor_usuario_db(user_id)
except:
    body = {
    "message": f"{event}"
    }
    response = {
    "statusCode": 500,
    "body": json.dumps(body)
    }
    print(response)

recommended_pets = return_recommended_pets(user, 10, 2)
recommended_pets_arrays = [retorna_vetor_pet(pet) for pet in recommended_pets]
validation_array = return_validation_array(recommended_pets_arrays)
pets_recomendados = knn_db(validation_array)

lista_pets = [pet["_id"] for pet in pets_recomendados.index]
fim = time.time()

print(f'Tempo pra iniciar o banco: {mongo_time - inicio}')
print(f'Tempo total: {fim - inicio}')

string_lista_pets = [str(pet) for pet in lista_pets]

body = {
    "message": string_lista_pets
}

response = {
    "statusCode": 200,
    "headers": {
        "Content-Type": "application/json"
    },
    "body": json.dumps(body)
}


print(response)
