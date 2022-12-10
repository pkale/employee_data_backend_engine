import os
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
from sqlalchemy.sql import text
from dotenv import load_dotenv
import psycopg2
import uuid
import models
from load import Loader


load_dotenv() 

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
db = SQLAlchemy(app)

print ('\nLoading data into db tables . . . ')
loader = Loader()
loader.load_data()
print ('Done!')
session = loader.session

POSTGRES_DB = os.getenv('DATABASE')
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
POSTGRES_USER = os.getenv('USER')
POSTRES_PW = os.getenv('PASSWORD')



connection = psycopg2.connect(
    host=HOST,
    port=PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTRES_PW,
)
connection.set_session(autocommit=True)

print('\n\nFlask app is ready to use. \n\n')
    
GET_INVESTORS = ("""
SELECT c.investors FROM companies c
	LEFT JOIN company_li_names ON c.company_name = company_li_names.company_name
WHERE 
	company_li_names.company_li_name =(%s) and c.investors is not null
GROUP BY
	c.investors""")

GET_INVESTORS_ORM = ("""
SELECT c.investors FROM companies c
	LEFT JOIN company_li_names ON c.company_name = company_li_names.company_name
WHERE 
	company_li_names.company_li_name =:company_li_name and c.investors is not null
GROUP BY
	c.investors; VALUES (:company_li_name)""")

GET_COMPANIES = ("""
SELECT people.company_name FROM people 
WHERE person_id =(%s); """) 

GET_COMPANIES_ORM = ("""
SELECT people.company_name FROM people 
WHERE person_id =:person_id; VALUES(:person_id)""") 


GET_AVG_FUNDING_BY_PERSON = ("""
SELECT
	avg(companies.known_total_funding)
FROM
	people
	INNER JOIN company_li_names ON people.company_li_name = company_li_names.company_li_name
	INNER JOIN companies ON company_li_names.company_name = companies.company_name
WHERE (companies.company_name = people.company_name
	OR company_li_names.company_li_name = people.company_li_name)
AND person_id = (%s)
""")

GET_AVG_FUNDING_BY_PERSON_ORM = ("""
SELECT
	avg(companies.known_total_funding) as avg
FROM
	people
	INNER JOIN company_li_names ON people.company_li_name = company_li_names.company_li_name
	INNER JOIN companies ON company_li_names.company_name = companies.company_name
WHERE (companies.company_name = people.company_name
	OR company_li_names.company_li_name = people.company_li_name)
AND person_id = :person_id; VALUES(:person_id)
""")


@app.get('/')
def home(): 
    return ('Hello World!')

@app.get('/companies-by-person')
def get_companies_by_person():

    data = request.get_json()
    person_id = data['person_id']
    try:
        uuid.UUID(str(person_id))
    except ValueError:
        return {"statusCode": 400, "message": 'Invalid uuid person_id: ' +person_id}
    with connection: 
        with connection.cursor() as cursor: 
            cursor.execute(GET_COMPANIES,(person_id,))
            companies = [item for sublist in cursor.fetchall() for item in sublist]
    return {"statusCode":200, "companies":companies, "message":'Successfully completed the request for person_id '+person_id}



@app.get('/avg-funding-by-person')
def get_average_funding_by_person(): 
    try:
        data = request.get_json() 
    except:
        return {"statusCode": 400, "message":"Invalid number of parameters."}
    person_id = data['person_id']
    try:
        uuid.UUID(str(person_id))
    except ValueError:
        return {"statusCode": 400, "message": 'Invalid uuid person_id: ' +person_id}   
    with connection: 
        with connection.cursor() as cursor: 
            cursor.execute(GET_AVG_FUNDING_BY_PERSON,(person_id,))
            avg_funding = cursor.fetchone()[0]
    return {"statusCode":200, "average_funding":avg_funding, "message":'Successfully completed the request for person_id '+person_id}



@app.get('/investors-by-company')
def get_investors_by_company_li_name():
    try:
        data = request.get_json() 
    except:
        return {"statusCode": 400, "message": 'Incorrection number of parameters.' }
    company_li_name = data['company_li_name']
    with connection: 
        with connection.cursor() as cursor: 
            cursor.execute(GET_INVESTORS, (company_li_name,))
            investors = cursor.fetchone()[0]
            investors = investors.replace('\n','').replace('"','').replace(' ','').lstrip('[').lstrip(']').split(',')
    return {"investors": investors}


