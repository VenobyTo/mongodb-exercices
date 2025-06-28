from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["schoolDB"]
classes = db["classes"]

# Insertion d'un document
classes.insert_one({
    "className": "Mathematics 101",
    "professor": "John Doe",
    "students": [
        {
            "name": "Charlie",
            "age": 21,
            "grades": { "midterm": 79, "final": 92 }
        },
        {
            "name": "Dylan",
            "age": 23,
            "grades": { "midterm": 79, "final": 87 }
        }
    ]
})

# Rechercher étudiants avec note finale > 85
print("Étudiants avec note finale > 85 :")
for doc in classes.find({ "students.grades.final": { "$gt": 85 } }):
    print(doc)

# Mise à jour : augmenter la note de Bob
classes.update_one(
    { "className": "Mathematics 101", "students.name": "Bob" },
    { "$inc": { "students.$.grades.final": 5 } }
)

# Ajouter un étudiant (Charlie)
classes.update_one(
    { "className": "Mathematics 101" },
    { "$push": {
        "students": {
            "name": "Charlie",
            "age": 23,
            "grades": { "midterm": 82, "final": 88 }
        }
    }}
)

# Supprimer l'étudiant Alice
classes.update_one(
    { "className": "Mathematics 101" },
    { "$pull": { "students": { "name": "Alice" } } }
)

# Agrégation : moyenne des notes finales
avg_pipeline = [
    { "$match": { "className": "Mathematics 101" } },
    { "$unwind": "$students" },
    { "$group": {
        "_id": "$className",
        "avgFinal": { "$avg": "$students.grades.final" }
    }}
]
print("Moyenne finale :")
print(list(classes.aggregate(avg_pipeline)))

# Agrégation : note maximale
max_pipeline = [
    { "$match": { "className": "Mathematics 101" } },
    { "$unwind": "$students" },
    { "$group": {
        "_id": "$className",
        "maxFinal": { "$max": "$students.grades.final" }
    }}
]
print("Note maximale :")
print(list(classes.aggregate(max_pipeline)))
