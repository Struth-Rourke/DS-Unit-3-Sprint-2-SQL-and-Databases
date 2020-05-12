import os 
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)


#DATABASE_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "data", "chinook_python.db")
DATABASE_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")

## Connection
conn = sqlite3.connect(DATABASE_FILEPATH)
conn.row_factory = sqlite3.Row
#print(type(conn))

## Cursor
curs = conn.cursor()
#print(type(curs))

### Queries
## How many total Characters are there?
query1 = """
SELECT 
    count(distinct character_id) as number_of_characters 

FROM charactercreator_character
"""
# ANSWER:
result = curs.execute(query1).fetchall()
print("Total Characters:", result[0][0])
print('\n')


## How many of each specific subclass?
# Cleric
query_cleric = """
SELECT 
	count(distinct character_id) as number_of_clerics
FROM(	
	SELECT
		charactercreator_character.character_id
		,charactercreator_character.name
		
	FROM charactercreator_character
	JOIN charactercreator_cleric ON charactercreator_cleric.character_ptr_id = 
	 charactercreator_character.character_id
	GROUP BY character_id
)
"""
# ANSWER:
result = curs.execute(query_cleric).fetchall()
print("Number of Clerics:", result[0][0]) 
print('\n')


# Fighter
query_fighter = """
SELECT 
	count(distinct character_id) as number_of_fighters
FROM(	
	SELECT
		charactercreator_character.character_id
		,charactercreator_character.name
		
	FROM charactercreator_character
	JOIN charactercreator_fighter ON charactercreator_fighter.character_ptr_id = 
	 charactercreator_character.character_id
	GROUP BY character_id
)
"""
# ANSWER:
result = curs.execute(query_fighter).fetchall()
print("Number of Fighters", result[0][0]) 
print('\n')


# Mage -- Necromancer is a subset
query_mage = """
SELECT 
	count(distinct character_id) as number_of_mages
FROM(	
	SELECT
		charactercreator_character.character_id
		,charactercreator_character.name
		
	FROM charactercreator_character
	JOIN charactercreator_mage ON charactercreator_mage.character_ptr_id = 
	 charactercreator_character.character_id
	GROUP BY character_id
)
"""
# ANSWER:
result = curs.execute(query_mage).fetchall()
print("Number of Mages:", result[0][0]) 
print('\n')


# Thief
query_thief = """
SELECT 
	count(distinct character_id) as number_of_thieves
FROM(	
	SELECT
		charactercreator_character.character_id
		,charactercreator_character.name
		
	FROM charactercreator_character
	JOIN charactercreator_thief ON charactercreator_thief.character_ptr_id = 
	 charactercreator_character.character_id
	GROUP BY character_id
)
"""
# ANSWER:
result = curs.execute(query_thief).fetchall()
print("Number of Thieves:", result[0][0]) 
print('\n')


## How many total Items?
query_items_total = """
SELECT 
	count(distinct item_id) as total_items

FROM armory_item
"""
result = curs.execute(query_items_total).fetchall()
print("Total Items:", result[0][0]) 
print('\n')


## How many of the Items are weapons? How many are not?
query_weapons = """
SELECT
	count(distinct item_ptr_id) as weapon_items
	,174 - 37 as items_not_weapons
FROM armory_weapon
"""
result = curs.execute(query_weapons).fetchall()
print("Items that are Weapons:", result[0][0]) 
print('\n')

## How many Items does each character have? (Return first 20 rows)
query_items_per_character = """
SELECT
	character_id
	,count(distinct item_id) as items_per
FROM charactercreator_character_inventory
GROUP BY character_id
LIMIT 20
"""
result = curs.execute(query_items_per_character).fetchall()
var = [list(x) for x in result]
print(var)
print('\n')



## How many Weapons does each character have? (Return first 20 rows)
query_weapon_per_character = """
SELECT
	character_id
	,count(distinct item_id) as item_per
FROM charactercreator_character_inventory
JOIN armory_weapon ON armory_weapon.item_ptr_id = charactercreator_character_inventory.item_id
GROUP BY character_id
LIMIT 20
"""
result = curs.execute(query_weapon_per_character).fetchall()
var = [list(x) for x in result]
print(var)
print('\n')


## On average, how many Items does each Character have?
query_avg_items = """
SELECT
	AVG(items_per) as avg_items
FROM(
	SELECT
		character_id
		,count(distinct item_id) as items_per
	FROM charactercreator_character_inventory
	GROUP BY character_id
)
"""
result = curs.execute(query_avg_items).fetchall()
print("Average Items:", result[0][0])
print('\n')


## On average, how many Weapons does each character have?
query_avg_weapons = """
SELECT
	AVG(weapons_per) as avg_weapons
FROM(
	SELECT
		character_id
		,count(distinct item_ptr_id) as weapons_per
	FROM charactercreator_character_inventory
	LEFT JOIN armory_weapon ON armory_weapon.item_ptr_id = charactercreator_character_inventory.item_id
	GROUP BY character_id
)
"""
result = curs.execute(query_avg_weapons).fetchall() #> NEED to have 'Fetch' returns a LIST
print("Average Weapons:", result[0][0])
print('\n')



##### PART 2 #####
# Read CSV
df = pd.read_csv('buddymove_holidayiq.csv')
print(df.shape)
print(df.head())
print('\n')

# Convert to SQLite3
path = os.path.join(os.path.dirname(__file__), "buddymove_holidayiq.sqlite3")
db = sqlite3.connect(path)
#print("CONNECTION:", connection)
cursor = db.cursor()
df.to_sql("review", con=db, if_exists="replace", index=False,
          dtype={"user_id": "TEXT",
                 "sports": "INTEGER",
                 "religious": "INTEGER",
                 "nature": "INTEGER",
                 "theatre": "INTEGER",
                 "shopping": "INTEGER",
                 "picnic": "INTEGER"})

# Buddy Holiday.py
query_row_count = """
SELECT
	count(distinct "User Id") as usder_id_count
FROM review
"""
result = cursor.execute(query_row_count).fetchall() 
print("Row Count:", result[0][0])
print('\n')


query_nature_shopping = """
SELECT
	count(distinct "User Id") as usder_id_count
FROM review
WHERE
	"Nature" >= 100
	AND "Shopping" >= 100
"""
result = cursor.execute(query_nature_shopping).fetchall() 
print("Count Nature AND Shopping >= 100:", result[0][0])
print('\n')

query_avg_allcategories = """
SELECT 
    AVG(Sports)
    ,AVG(Religious)
    ,AVG(Nature)
    ,AVG(Theatre)
    ,AVG(Shopping)
    ,AVG(Picnic)
FROM REVIEW
"""
result = cursor.execute(query_avg_allcategories).fetchall() 
print("Count Nature AND Shopping >= 100:", result[0][0])
print('\n')

