from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["testDB"]
coll = db["testCollection"]

# Insertion
coll.insert_one({"name": "test", "value": 1})

# Lecture
print("Contenu de testCollection :")
print(list(coll.find()))

# Mise à jour
coll.update_one({"name": "test"}, {"$inc": {"value": 1}})

# Suppression document
coll.delete_one({"name": "test"})

# Suppression collection
coll.drop()

# Suppression base de données
client.drop_database("testDB")
