from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017")
db = client["PokemonDB"]
pokemons = db["Pokemons"]

# Importation CSV
df = pd.read_csv("pokemonGO.csv")
pokemons.insert_many(df.to_dict("records"))

# Lire tous les Pokémon de type Feu
print("Pokémon de type Feu :")
for p in pokemons.find({"Type1": "Fire"}):
    print(p)

# Lire les infos de Pikachu
print("\nInfos de Pikachu :")
print(pokemons.find_one({"Name": "Pikachu"}))

# Mise à jour CP de Pikachu
pokemons.update_one({"Name": "Pikachu"}, {"$set": {"CP": 900}})

# Supprimer Bulbasaur
pokemons.delete_one({"Name": "Bulbasaur"})
