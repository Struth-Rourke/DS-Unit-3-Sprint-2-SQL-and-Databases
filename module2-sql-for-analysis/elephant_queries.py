import os
import json
#import pandas as pd
import psycopg2 as psycho
from psycopg2.extras import execute_values # helper function

# For .env file
from dotenv import load_dotenv
load_dotenv() # loads content of the .env file into the scripts environment


# Environment Variables
DB_NAME = os.getenv("DB_NAME", default='OOPS')
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default='OOPS')
DB_HOST = os.getenv("DB_HOST", default='OOPS')

# print(DB_NAME)
# print(DB_USER)
# print(DB_PASSWORD)
# print(DB_HOST)

# exit() # quit()

# Connection Object
connection = psycho.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASSWORD, host=DB_HOST)
print(type(connection)) # <class 'psycopg2.extensions.connection'>


# Cursor Object
cursor = connection.cursor()
print(type(cursor)) # <class 'psycopg2.extensions.cursor'>
# breakpoint()


# Cursor Execute
cursor.execute("SELECT * FROM test_table;")

# Cursor Results
#results = cursor.fetchall()
#for row in results:
    # <class 'tuple'> (1, 'A row name', None)
    # <class 'tuple'> (2, 'Another row, with JSON', {'a': 1, 'b': ['dog', 'cat', 42], 'c': True})
#    print(type(row), row)



### INSERTS:
my_dict = { "a": 1, "b": ["dog", "cat", 42], "c": 'true' }

##### APPRAOCH 1:
##insertion_query = f"INSERT INTO test_table (name, data) VALUES (%s, %s)"
##cursor.execute(insertion_query,
##  ('A rowwwww', 'null')
##)
##cursor.execute(insertion_query,
##  ('Another row, with JSONNNNN', json.dumps(my_dict)) # Converting dictionary to string
##)

## actually save records / run the transaction
##connection.commit() 

## closes the connection
##cursor.close()
##connection.close()


# h/t: https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
insertion_query = f"INSERT INTO test_table (name, data) VALUES %s"
execute_values(cursor, insertion_query, [
  ('A rowwwww', 'null'),
  ('Another row, with JSONNNNN', json.dumps(my_dict)),
  ('Third row', "3")
]) # data as a list of tuples

## actually save records / run the transaction
connection.commit() 

## closes the connection
cursor.close()
connection.close()


