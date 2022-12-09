
import os
from sqlalchemy import Column, String, Numeric, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy import create_engine  

from dotenv import load_dotenv

load_dotenv() 
db_string = os.getenv('DATABASE_URL')
base = declarative_base()
db = create_engine(db_string)  

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