import os
import pymongo
from dotenv import load_dotenv
load_dotenv()


DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")



connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)


client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)
print(dir(client))
print("DB Names:", client.list_database_names()) #> [admin,local]


#breakpoint()


# Creating the DB; "test_database" 
db = client.test_database 
print("----------------")
print("DB:", type(db), db)


# Creating a collection; "pokemon_test"
collection = db.pokemon_test 
print("----------------")
print("COLLECTION:", type(collection), collection)

print("----------------")
print("COLLECTIONS:")
print(db.list_collection_names())


collection.insert_one({
    "name": "Pikachu",
    "level": 30,
    "exp": 76000000000,
    "hp": 400,
})
print("DOCS:", collection.count_documents({}))
print(collection.count_documents({"name": "Pikachu"}))


mewtwo = {
    "name": "Mewtwo",
    "level": 100,
    "exp": 76000000000,
    "hp": 450,
    "strength": 550,
    "intelligence": 450,
    "dexterity": 300,
    "wisdom": 575
}

blastoise = {
    "name": "Blastoise",
    "lvl": 70,
}

charmander = {
    "nameeeeee": "Charmander",
    "lvl": 70, # Mistake with structure (lvl vs. level)
    "random_stat": {'a':2}
}


skarmory = {
    "name": "Skarmory",
    "level": 22,
    "exp": 42000,
    "hp": 85,
    "strength": 750,
    "intelligence": 8,
    "dexterity": 57
}

cubone = {
    "name": "Cubone",
    "level": 20,
    "exp": 35000,
    "hp": 80,
    "strength": 600,
    "intelligence": 60,
    "dexterity": 200,
    "wisdom": 200
}

scyther = {
    "name": "Scyther",
    "level": 99,
    "exp": 7000,
    "hp": 40,
    "strength": 50,
    "intelligence": 40,
    "dexterity": 30,
    "wisdom": 57
}

slowpoke = {
    "name": "Slowpoke",
    "level": 1,
    "exp": 100,
    "hp": 80,
    "strength": 100,
    "intelligence": 10,
    "dexterity": 50,
    "wisdom": 200
}


pokemon_team = [mewtwo, blastoise, charmander, skarmory, cubone, scyther, slowpoke]
collection.insert_many(pokemon_team)

print("DOCS:", collection.count_documents({}))


#breakpoint()

pikas = list(collection.find({"name": "Pikachu"})) # SELECT * FROM pokemon WHERE name = "Pikachu"
print(len(pikas), "PIKAS")
print(pikas[0]["_id"]) # ObjectId# is created by Mongo Automatically
print(pikas[0]["name"])


strong = list(collection.find({"level": {"$gte": 60}}))
# TODO: try and account for lvl v. level

