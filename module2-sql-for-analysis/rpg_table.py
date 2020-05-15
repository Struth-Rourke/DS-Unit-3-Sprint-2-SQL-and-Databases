import os 
import json
import sqlite3
import pandas as pd

# For PostgreSQL
import psycopg2 as psycho
from psycopg2.extras import execute_values 
# .env for security -- load_dotenv necessary for python to actually go into 
# .env folder and load the information
from dotenv import load_dotenv
load_dotenv()


# os filepath 
DATABASE_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")


## SQLite Connection
lite_conn = sqlite3.connect(DATABASE_FILEPATH)
# Option to use row_factory
lite_conn.row_factory = sqlite3.Row
#print(type(conn))


## SQLite Cursor via lite_conn
lite_curs = lite_conn.cursor()
#print(type(curs))


## Queries
# How many total Characters are there?
query1 = """
SELECT *
FROM charactercreator_character
"""
# ANSWER:
lite_result = lite_curs.execute(query1).fetchall()
lite_var = [list(x) for x in lite_result]
#print(lite_var)
#print('\n')



##### Pandas #####
# Creating DataFrame
df = pd.DataFrame(lite_var, columns = ['id', 'names', 'level', 
                                       'exp', 'hp', 'strength',
                                       'iq', 'dexterity', 'wisdom'])
#print(df.head())



##### SQL #####
#df.to_sql("rpg_table", con=engine, if_exists="replace", index=False)
        ## ** DON'T NEED THIS: to_sql takes care of this **
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

from sqlalchemy import create_engine
DB_URL = os.getenv("DB_URL", default='OOPS')
engine = create_engine(DB_URL, echo=False)

## Read_csv of titanic
#titanic = pd.read_csv('titanic.csv')

## Converting to_sql 
#titanic.to_sql('titanic_table', con=engine, if_exists="replace", index=False)

## Environment Variables for PostgreSQL
DB_NAME = os.getenv("DB_NAME", default='OOPS')
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default='OOPS')
DB_HOST = os.getenv("DB_HOST", default='OOPS')


## PostgreSQL Connection Object
gres_conn = psycho.connect(dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD, host=DB_HOST)
#print(type(conn))


## PostgreSQL Cursor Object
gres_curs = gres_conn.cursor()
#print(type(curs)) 


### Queries ###
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
# RESULTS: query1
result = gres_curs.execute(query1)
### ** for PostgreSQL, you CANNOT combine .execute() and .fetchall() ** ###
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
# RESULTS: query2
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
# RESULTS: query3
result = gres_curs.execute(query3)
results = gres_curs.fetchall()
print(results)
print('\n')







##### MIKE SOLUTION 1: #####

# import os
# #import json
# import pandas
# import numpy as np

# import psycopg2 as psycho
# from psycopg2.extras import execute_values
### *** Helps with changing numpy.int64 to int4 for SQL *** ###
# psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# from dotenv import load_dotenv # python-dotenv
# load_dotenv() #> loads contents of the .env file into the script's environment



### READ PASSENGER DATA FROM THE CSV FILE
# #CSV_FILEPATH = "titanic.csv"
# #CSV_FILEPATH = os.path.join(os.path.dirname(__file__), "titanic.csv")
# CSV_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "module2-sql-for-analysis", "titanic.csv")
# df = pandas.read_csv(CSV_FILEPATH)
# print(df.dtypes)
# print(df.head())


### CONNECT TO THE PG DATABASE
# DB_NAME = os.getenv("DB_NAME", default="OOPS")
# DB_USER = os.getenv("DB_USER", default="OOPS")
# DB_PW = os.getenv("DB_PW", default="OOPS")
# DB_HOST = os.getenv("DB_HOST", default="OOPS")

# connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PW, host=DB_HOST)
# print(type(connection)) #> <class 'psycopg2.extensions.connection'>

# cursor = connection.cursor()
# print(type(cursor)) #> <class 'psycopg2.extensions.cursor'>


### CREATE A TABLE TO STORE THE PASSENGERS
# table_creation_sql = """
# DROP TABLE IF EXISTS passengers;
# CREATE TABLE IF NOT EXISTS passengers (
#     id  SERIAL PRIMARY KEY,
#     "survived" int4, -- consider boolean here
#     "pclass" int4,
#     "name" text,
#     "sex" text,
#     "age" int4,
#     "sib_spouse_count" int4,
#     "parent_child_count" int4,
#     "fare" float8
# );
# """
# cursor.execute(table_creation_sql)


### INSERT DATA INTO THE PASSENGERS TABLE
## how to convert dataframe to a list of tuples?
# list_of_tuples = list(df.to_records(index=False))
# insertion_query = f"INSERT INTO passengers (survived, pclass, name, sex, age, sib_spouse_count, parent_child_count, fare) VALUES %s"
# execute_values(cursor, insertion_query, list_of_tuples) 
# connection.commit() # actually save the records / run the transaction to insert rows
# cursor.close()
# connection.close()







##### MIKE SOLUTION 2: #####

# to get over errors about not being able to work with the numpy integer datatypes
# could alternatively change the datatypes of our dataframe,
# ... or do transformations on our list of tuples later (after reading from the dataframe, before inserting into the table)
# psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

### .env Name, User, Password and Host variables
# DB_NAME = os.getenv("DB_NAME")
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_HOST = os.getenv("DB_HOST")

### Read_CSV via os filepath
# CSV_FILEPATH = os.path.join(os.path.dirname(__file__), "titanic.csv")
# df = pandas.read_csv(CSV_FILEPATH)
# df.index += 1         # to start index at 1 (resembling primary key behavior)
# print(df.head())

### Connection to PostgreSQL 
# gres_conn = psycho.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
# print(type(gres_conn)) # <class 'psycopg2.extensions.connection'>

### Cursor to PostgreSQL
# gres_curs = connection.cursor()
# print(type(gres_cursor)) # <class 'psycopg2.extensions.cursor'>

### Querying SQL
# query = """SELECT * from test_table;"""
# cursor.execute(query)
# results = cursor.fetchall()
# print(type(results)) #> list
# print(results)


### Creating the table

## Table Creation Query
# table_creation_query = """
# DROP TABLE passengers;
# CREATE TABLE IF NOT EXISTS passengers (
#   id SERIAL PRIMARY KEY,
#   survived integer,
#   pclass integer,
#   name varchar NOT NULL,
#   gender varchar NOT NULL,
#   age float,
#   sib_spouse_count integer,
#   parent_child_count integer,
#   fare float
# );
# """
# cursor.execute(table_creation_query)


## Converting df into a list of tuples
# list_of_tuples = list(df.to_records(index=True))
# sometimes would need to do further transformations (list comprehension,etc.)

## Creating insertion query
# insertion_query = """
# INSERT INTO passengers (id, survived, pclass, name, gender, age, sib_spouse_count, parent_child_count, fare) VALUES %s
# """

## Executing values
# execute_values(cursor, insertion_query, list_of_tuples)

## Saving results via commit
# connection.commit()

## Closing connection
# cursor.close()
# connection.close()







##### ALTERNATE SOLUTION: #####

## SQLite execution and check
#q1 = lite_curs.execute(get_first_table).fetchall()
#print(q1[0])

## read_sql and check
#armory_items = pd.read_sql(sql=get_first_table, con=lite_conn)
#print(armory_items)

## PostgreSQL connection and cursor
#gres_conn = psycho.connect(dbname=DB_NAME, user=DB_USER, password=DB_PW, host=DB_HOST)
#gres_curs = gres_conn.cursor()

## Creating the Table
#create_table = '''
#create table if not exists armory_items(
#    item_id INTEGER NOT NULL PRIMARY KEY,
#    name    varchar(200),
#    value   INTEGER,
#    weight  INTEGER
#)
#'''

## Commit changes which actually creates the table
# gres_curs.execute(create_table)
# gres_curs.fetchall()
## ** NEED to break these two steps apart for PostgreSQL; **
## ** [ex.] can't be .execute(something).fetchall() **

# gres_conn.commit()

# # insertion query string
# insertion_query = f"INSERT INTO armory_items (item_id, name, value, weight) VALUES %s"
# # use insertion query above and q1 (first query), to insert table into postgresql

# execute_values(gres_curs, insertion_query, q1)
# gres_conn.commit()
# gres_curs.close()
# gres_conn.close()