import os 
import sqlite3
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

##------------------------------------------------------------------------------
# 1. Reading in a specific SQLite3 Table
##------------------------------------------------------------------------------

# os filepath 
DATABASE_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")

# SQLite Connection
lite_conn = sqlite3.connect(DATABASE_FILEPATH)

# SQLite Cursor via lite_conn
lite_curs = lite_conn.cursor()

# Query: Grab all the Characters from the charactercreator_character file
query1 = """
SELECT *
FROM charactercreator_character
"""
# ANSWER:
lite_result = lite_curs.execute(query1).fetchall()
lite_var = [list(x) for x in lite_result]
#print(lite_var)
#print('\n')


##------------------------------------------------------------------------------
# 2. Creating a DataFrame from the data
##------------------------------------------------------------------------------

# Creating DataFrame
df = pd.DataFrame(lite_var, columns = ['id', 'names', 'level', 
                                       'exp', 'hp', 'strength',
                                       'iq', 'dexterity', 'wisdom'])

#print(df.head())


##------------------------------------------------------------------------------
# 3. Connecting and Inserting a DataFrame in Mongo DB 
##------------------------------------------------------------------------------

# Importing Packages
import pymongo
import json


# MongoDB Credentials
DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")

# Conncetion URI
connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"

# Mongo Client 
client = pymongo.MongoClient(connection_uri)

# Creating the DataBase; "rpg_database" 
db = client.rpg_database

# Creating a Collection; "rpg"
collection = db.rpg

# Insert into Collection
records = json.loads(df.to_json(orient='records'))
db.collection.insert_one(records)
print(db.list_collection_names())

