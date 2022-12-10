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

GET_COMPANIES = ("""
SELECT people.company_name FROM people 
WHERE person_id =(%s); """) 


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



@app.get('/')
def home(): 
    return ('Hello World!')

@app.get('/companies-by-person/<given_person_id>')
def get_companies_by_person(given_person_id):
    person_id = None
    if given_person_id: 
        person_id = given_person_id
    else: 
        data = request.get_json()
        person_id = data['person_id']
    try:
        uuid.UUID(str(person_id))
    except ValueError:
        return {"statusCode": 400, "Errors" : {"message": 'Bad Request: Invalid uuid person_id: ' +person_id}}
    
    exists = db.session.query(models.Person.person_id).filter_by(person_id=person_id).first() is not None
    if not exists: 
            return { "investors": [], "statusCode":404, "Errors": {"Message": "Not found: Person_id does not exist in the db."}}
    
    with connection: 
        with connection.cursor() as cursor: 
            cursor.execute(GET_COMPANIES,(person_id,))
            companies = [item for sublist in cursor.fetchall() for item in sublist]
    return {"statusCode":200, "Response":{"companies":companies}, "Errors": {}}



@app.get('/avg-funding-by-person/<given_person_id>')
def get_average_funding_by_person(given_person_id): 
    person_id = None
    if given_person_id: 
        person_id = given_person_id
    else: 
        try:
            data = request.get_json() 
            person_id = data['person_id']
        except:
            return {"statusCode": 400, "message":"Bad Request: Invalid number of parameters."}
    try:
        uuid.UUID(str(person_id))
    except ValueError:
        return {"statusCode": 400, "Errors": { "message": 'Invalid uuid person_id: ' +person_id}}
    
    exists = db.session.query(models.Person.person_id).filter_by(person_id=person_id).first() is not None
    if not exists: 
            return { "investors": [], "statusCode":404, "Errors": {"Message": "Not Found: Person_id does not exist in the db."}}
    
    with connection: 
        with connection.cursor() as cursor: 
            cursor.execute(GET_AVG_FUNDING_BY_PERSON,(person_id,))
            avg_funding = cursor.fetchone()[0]

    return {"statusCode":200, "Response":{"avg_funding":avg_funding}, "Errors":{}}



@app.get('/investors-by-company/<given_company_li_name>')
def get_investors_by_company_li_name(given_company_li_name):
    company_li_name = None
    if given_company_li_name: 
        company_li_name = given_company_li_name
    exists = db.session.query(models.Company_Li_Mapping.company_name).filter_by(company_li_name=company_li_name).first() is not None
    if exists is None: 
            return { "Response: ": [], "statusCode":404, "Errors": {"Message": "Not Found: Company_li_name does not exist in the db."}}
    with connection: 
        with connection.cursor() as cursor: 
            cursor.execute(GET_INVESTORS, (company_li_name,))
            investors = cursor.fetchone()
            if investors is None: 
                investors = []
            else: 
                investors = investors[0].replace('\n','').replace('"','').replace(' ','').lstrip('[').lstrip(']').split(',')
    return { "Response": {"investors":investors}, "statusCode":200, "Errors": {}}


