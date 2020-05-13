import os 
import json
import sqlite3
import pandas as pd
import psycopg2 as psycho
from psycopg2.extras import execute_values 
from dotenv import load_dotenv
load_dotenv()

# SQLalchemy
from sqlalchemy import create_engine

DB_URL = os.getenv("DB_URL", default='OOPS')
engine = create_engine(DB_URL, echo=False)

# os filepath
DATABASE_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")

## Connection
lite_conn = sqlite3.connect(DATABASE_FILEPATH)
lite_conn.row_factory = sqlite3.Row
#print(type(conn))

## Cursor
lite_curs = lite_conn.cursor()
#print(type(curs))

### Queries
## How many total Characters are there?
query1 = """
SELECT *
FROM charactercreator_character
"""
# ANSWER:
lite_result = lite_curs.execute(query1).fetchall()
#print('\n')
lite_var = [list(x) for x in lite_result]
#print(var)
#print('\n')

#exit()

##### Pandas #####
df = pd.DataFrame(lite_var, columns = ['id', 'names', 'level', 
                                       'exp', 'hp', 'strength',
                                       'iq', 'dexterity', 'wisdom'])
#print(df.head())


##### SQL #####
#df.to_sql("rpg_table", con=engine, if_exists="replace", index=False)
        #   #dtype={"id": "INTEGER",
        #          "names": "VARCHAR(50)",
        #          "level": "INTEGER",
        #          "exp": "INTEGER",
        #          "hp": "INTEGER",
        #          "strength": "INTEGER",
        #          "iq": "INTEGER",
        #          "dexterity":"INTEGER",
        #          "wisdom":"INTEGER"})

##### Titanic #####
#titanic = pd.read_csv('titanic.csv')
#titanic.to_sql('titanic_table', con=engine, if_exists="replace", index=False)

# Environment Variables
DB_NAME = os.getenv("DB_NAME", default='OOPS')
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default='OOPS')
DB_HOST = os.getenv("DB_HOST", default='OOPS')


# PostgreSQL Connection Object
gres_conn = psycho.connect(dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD, host=DB_HOST)
#print(type(conn))

# PostgreSQL Cursor Object
gres_curs = gres_conn.cursor()
#print(type(curs)) 

## Queries
# query1
query1 = """
SELECT 
	"Sex"
	,count("Sex") as Sex_count
	,AVG("Age") as Sex_AVG_Age
	,AVG("Fare") as Sex_AVG_Fare
FROM titanic_table
GROUP BY "Sex"
"""

# Results
result = gres_curs.execute(query1)
results = gres_curs.fetchall()
print('\n')
print(results)
print('\n')


# query2
query2 = """
SELECT 
	AVG("Survived") as Average_Survival_Rate
	,AVG("Fare") as Average_Fare
	,AVG("Age") as Average_Age
FROM titanic_table
"""

# Results
result = gres_curs.execute(query2)
results = gres_curs.fetchall()
print(results)
print('\n')


# query3
query3 = """
SELECT
	"Pclass"
	,count("Pclass") as Pclass_class
	,AVG("Fare") as Pclass_Fare_AVG
FROM titanic_table
GROUP BY "Pclass"
"""

# Results
result = gres_curs.execute(query3)
results = gres_curs.fetchall()
print(results)
print('\n')




##### ALTERNATE SOLUTION: #####
# q1 = lite_curs.execute(get_first_table).fetchall()
# print(q1[0])
# armory_items = pd.read_sql(sql=get_first_table, con=lite_conn)
# print(armory_items)
# gres_conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PW, host=DB_HOST)
# gres_curs = gres_conn.cursor()
# create_table = '''
# create table if not exists armory_items(
#     item_id INTEGER NOT NULL PRIMARY KEY,
#     name    varchar(200),
#     value   INTEGER,
#     weight  INTEGER
# )
# '''
# # actually create the table
# gres_curs.execute(create_table)
# # commit the created table
# gres_conn.commit()
# # insertion query string
# insertion_query = f"INSERT INTO armory_items (item_id, name, value, weight) VALUES %s"
# # use insertion query above and q1 (first query), to insert table into postgresql
# execute_values(gres_curs, insertion_query, q1)
# gres_conn.commit()
# gres_curs.close()
# gres_conn.close()