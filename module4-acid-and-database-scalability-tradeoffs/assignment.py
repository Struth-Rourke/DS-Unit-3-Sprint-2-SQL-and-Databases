import os 
import pandas as pd

# For PostgreSQL
import psycopg2 as psyco
from psycopg2.extras import execute_values 
from dotenv import load_dotenv
load_dotenv()

##------------------------------------------------------------------------------
# 1. Read in the csv and convert it to an SQL file
##------------------------------------------------------------------------------

## Read_csv of titanic
#titanic = pd.read_csv('titanic.csv')

## Converting to_sql 
#titanic.to_sql('titanic_table', con=engine, if_exists="replace", index=False)

##------------------------------------------------------------------------------
# 2. Getting .env credentials for PostgreSQL
##------------------------------------------------------------------------------

## Environment Variables for PostgreSQL
DB_NAME = os.getenv("DB_NAME", default='OOPS')
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default='OOPS')
DB_HOST = os.getenv("DB_HOST", default='OOPS')

##------------------------------------------------------------------------------
# 3. Creating PostgreSQL Connection and Cursor object to run queries from Python
##------------------------------------------------------------------------------

## PostgreSQL Connection Object
gres_conn = psyco.connect(dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD, host=DB_HOST)
#print(type(conn))


## PostgreSQL Cursor Object
gres_curs = gres_conn.cursor()
#print(type(curs)) 

##------------------------------------------------------------------------------
# 4. SQQL Queries
##------------------------------------------------------------------------------

# Q1: How many passengers survived, and how many died?
query1 = """
SELECT
	"Survived"
	,COUNT("Survived") as person_count
FROM titanic_table
GROUP BY "Survived"
"""
# RESULTS:
result = gres_curs.execute(query1)
### ** for PostgreSQL, you CANNOT combine .execute() and .fetchall() ** ###
results = gres_curs.fetchall()
print('\n')
print("Death/Count:", results[0], "Survival/Count:", results[1])
print('\n')
# for x in results:
# 	print("Survival/Death:", str(x[0]), "Count:", str(x[1]))




# Q2: How many passengers were in each class?
query2 = """
SELECT
	"Pclass"
	,count("Pclass") AS Pclass_count
FROM titanic_table
GROUP BY "Pclass"
ORDER BY "Pclass"
"""
# RESULTS:
result = gres_curs.execute(query2)
results = gres_curs.fetchall()
for y in results:
	print("PClass:", y[0], "Count:", y[1])
print('\n')



# Q3: How many passengers survived/died within each class?
query3 = """
SELECT
	"Pclass"
	,"Survived"
	,COUNT("Pclass")
FROM titanic_table
GROUP BY "Pclass", "Survived"
ORDER BY "Pclass", "Survived"
"""
# RESULTS:
result = gres_curs.execute(query3)
results = gres_curs.fetchall()
for z in results:
	print("PClass:", z[0], "Survival/Death:", z[1], "Count:", z[2])
print('\n')


# Q4: What was the average age of survivors vs nonsurvivors?
query4 = """
SELECT
	"Survived"
	,AVG("Age") AS Average_Age
FROM titanic_table
GROUP BY "Survived"
"""
# RESULTS:
result = gres_curs.execute(query4)
results = gres_curs.fetchall()
for i in results:
	print("Survivor/Nonsurvivor:", i[0], "Avg.Age:", i[1])
print('\n')



# Q5: What was the average age of each passenger class?
query5 = """
SELECT
	"Pclass"
	,AVG("Age") AS Pclass_AVG_Age
FROM titanic_table
GROUP BY "Pclass"
"""
# RESULTS:
result = gres_curs.execute(query5)
results = gres_curs.fetchall()
for j in results:
	print("PClass:", j[0], "Avg.Age:", j[1])
print('\n')



# Q6: What was the average fare by passenger class? By survival?
query6 = """
SELECT
	"Pclass"
	,"Survived"
	,AVG("Fare") AS AVG_fare
FROM titanic_table
GROUP BY "Pclass", "Survived"
ORDER BY "Pclass", "Survived"
"""
# RESULTS:
result = gres_curs.execute(query6)
results = gres_curs.fetchall()
for q in results:
	print("PClass:", q[0], "Survival/Death:", q[1], "Avg.Fare:", q[2])
print('\n')



# Q7: How many siblings/spouses aboard on average, by passenger class? By survival?
query7 = """
SELECT 
	"Pclass"
	,"Survived"
	,AVG("Siblings/Spouses Aboard") as AVG_Siblings
FROM titanic_table
GROUP BY "Pclass", "Survived"
ORDER BY "Pclass", "Survived"
"""
# RESULTS:
result = gres_curs.execute(query7)
results = gres_curs.fetchall()
for x in results:
	print("PClass:", x[0], "Survival/Death:", x[1], 
		  "Avg. Siblings/Spouses Aboard:", x[2])
print('\n')



# Q8: How many parents/children aboard on average, by passenger class? By survival?
query8 = """
SELECT 
	"Pclass"
	,"Survived"
	,AVG("Parents/Children Aboard") as AVG_Parents_Children
FROM titanic_table
GROUP BY "Pclass", "Survived"
ORDER BY "Pclass", "Survived"
"""
# RESULTS:
result = gres_curs.execute(query8)
results = gres_curs.fetchall()
for x in results:
	print("PClass:", x[0], "Survival/Death:", x[1], 
		  "Avg. Parents/Children Board:", x[2])
print('\n')



# Q9: Do any passengers have the same name?
query9 = """
SELECT
	"Name"
    ,COUNT(DISTINCT "Name") AS name_count
FROM titanic_table
GROUP BY "Name"
ORDER BY name_count DESC
LIMIT 1
"""
# RESULTS:
result = gres_curs.execute(query9)
results = gres_curs.fetchall()
print(results)
print('\n')