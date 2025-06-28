from pymongo import MongoClient
import pandas as pd
import re

# Connexion MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["TitanicDB"]
collection = db["Passengers"]

# ⚠️ Nettoyage si relance du script
collection.drop()

# === Exercice 1 : Importation des données ===
df = pd.read_csv("titanic.csv")
data = df.to_dict(orient="records")
collection.insert_many(data)

# === Exercice 2 : Analyse des données ===

# 1. Nombre total de passagers
total_passengers = collection.count_documents({})
print(f"Total de passagers : {total_passengers}")

# 2. Nombre de survivants (Survived == 1)
survivors = collection.count_documents({"Survived": 1})
print(f"Nombre de survivants : {survivors}")

# 3. Nombre de passagères femmes (Sex == 'female')
females = collection.count_documents({"Sex": "female"})
print(f"Nombre de femmes : {females}")

# 4. Passagers avec au moins 3 enfants (Parch >= 3)
with_kids = collection.count_documents({"Parch": {"$gte": 3}})
print(f"Passagers avec au moins 3 enfants : {with_kids}")

# === Exercice 3 : Mise à jour de données ===

# 1. Port d’embarquement manquant → 'S' (Southampton)
collection.update_many(
    {"Embarked": {"$in": [None, ""]}},
    {"$set": {"Embarked": "S"}}
)

# 2. Ajouter champ rescued = true pour survivants
collection.update_many(
    {"Survived": 1},
    {"$set": {"rescued": True}}
)

# === Exercice 4 : Requêtes complexes ===

# 1. 10 passagers les plus jeunes
youngest = collection.find(
    {"Age": {"$ne": None}},
    {"Name": 1, "Age": 1}
).sort("Age", 1).limit(10)
print("\n10 passagers les plus jeunes :")
for p in youngest:
    print(f"{p.get('Name')} - {p.get('Age')} ans")

# 2. Passagers non survivants en 2e classe
second_class_victims = collection.find(
    {"Survived": 0, "Pclass": 2},
    {"Name": 1}
)
print("\nPassagers décédés en 2e classe :")
for p in second_class_victims:
    print(p.get("Name"))

# === Exercice 5 : Suppression ===

# Supprimer ceux sans âge et non survivants
deleted_count = collection.delete_many(
    {"Survived": 0, "$or": [{"Age": None}, {"Age": ""}]}
).deleted_count
print(f"\n{deleted_count} documents supprimés (non survivants sans âge)")

# === Exercice 6 : Mise à jour en masse ===

# Ajouter 1 an à tous les âges connus
collection.update_many(
    {"Age": {"$type": "double"}},
    {"$inc": {"Age": 1}}
)

# === Exercice 7 : Suppression conditionnelle ===

# Supprimer ceux sans ticket
deleted_ticket = collection.delete_many(
    {"$or": [{"Ticket": {"$exists": False}}, {"Ticket": ""}]}
).deleted_count
print(f"{deleted_ticket} documents supprimés (sans ticket)")

# === Bonus : Regex pour Dr. ===

doctors = collection.find(
    {"Name": {"$regex": r"Dr\.", "$options": "i"}},
    {"Name": 1}
)
print("\nPassagers avec le titre Dr. :")
for d in doctors:
    print(d["Name"])
