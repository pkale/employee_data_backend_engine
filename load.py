import os
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Numeric, Boolean, Date, ARRAY
from sqlalchemy.ext.declarative import declarative_base  

from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from typing import Iterator, Dict, Any
import utils
db_string = "postgresql://admin:admin@localhost:5432/template1"

db = create_engine(db_string)  
base = declarative_base()

class Person(base):  
    __tablename__ = 'people'
    id = Column(Numeric, primary_key=True)
    person_id = Column(String)
    company_name = Column(String)
    company_li_name = Column(String)
    last_title = Column(String)
    is_founder = Column(Boolean)
    group_start_date = Column(Date)
    group_end_date = Column(Date)

class Company(base): 
    __tablename__ ='companies'
    id = Column(Numeric,primary_key=True)
    company_name=Column(String)
    company_li_name = Column(String)
    description=Column(String)
    headcount=Column(Numeric)
    founding_date=Column(Date)
    most_recent_raise=Column(Numeric)
    most_recent_valuation=Column(Numeric)
    known_total_funding =Column(Numeric)

class Company_Li_Mapping(base): 
    __tablename__ ='company_li_names'
    id = Column(Numeric,primary_key=True)
    company_name=Column(String)
    company_li_name = Column(String)


Session = sessionmaker(db)  
session = Session()

base.metadata.drop_all(db)
base.metadata.create_all(db)

load_dotenv() 
people_path = os.getenv('PEOPLE_BULK_DATA')
people = list(utils.iter_people_from_file(people_path))

companies_path = os.getenv('COMPANIES_BULK_DATA')
companies = list(utils.iter_companies_from_file(companies_path))

company_li_mappings = list(utils.iter_company_li_from_file(companies_path))




# Bulk upload people data 
for person_data in people: 
    
    person_id = person_data['person_id']
    company_name = person_data['company_name'].lower()
    company_li_name = utils.parse_li_name(person_data['company_li_name'])
    last_title = person_data['last_title'].lower().strip()
    is_founder = utils.parse_title_for_founder(last_title)
    group_start_date = utils.parse_date(person_data['group_start_date'])
    group_end_date = utils.parse_end_date(person_data['group_end_date'])
    
    person = Person(
        person_id=person_id, 
        company_name=company_name, 
        company_li_name=company_li_name,
        last_title=last_title,
        is_founder=is_founder,
        group_start_date = group_start_date,
        group_end_date = group_end_date
        )  
    session.add(person)  
session.commit()

# Bulk upload company data 
for company_data in companies: 
    
    company_name = company_data['company_name'].lower()
    company_li_name = utils.parse_lists(company_data['company_li_names'])
    description = utils.parse_strings(company_data['description'])
    headcount = utils.parse_numeric_value(company_data['headcount'])
    most_recent_raise = utils.parse_numeric_value(company_data['most_recent_raise'])
    most_recent_valuation = utils.parse_numeric_value(company_data['most_recent_valuation'])
    founding_date = utils.parse_date(company_data['founding_date'])
    known_total_funding = utils.parse_numeric_value(company_data['known_total_funding'])
    
    company = Company(
        company_name=company_name, 
        company_li_name=company_li_name,
        description = description,
        headcount = headcount,
        most_recent_raise = most_recent_raise,
        most_recent_valuation = most_recent_valuation,
        founding_date= founding_date,
        known_total_funding = known_total_funding
        )  
    session.add(company)  
session.commit()

for company_li_data in company_li_mappings: 
    company_li_name = company_li_data['company_li_name']
    company_name = utils.parse_strings(company_li_data['company_name'])

    company_li_mapping = Company_Li_Mapping(
        company_name = company_name,
        company_li_name = company_li_name
    )
    session.add(company_li_mapping)
session.commit()


# # Read
# people = session.query(Person)  
# for person in people:  
#     print(person.person_id)

