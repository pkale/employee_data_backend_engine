import os
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Numeric, Boolean, Date, ARRAY
from sqlalchemy.ext.declarative import declarative_base  
from models import Person, Company, Company_Li_Mapping, db, base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from typing import Iterator, Dict, Any
import utils

load_dotenv() 

class Loader(): 
    def __init__(self):
        self.session = None

    def load_data(self):
        # Create and store esson
        Session = sessionmaker(db)  
        session = Session()
        self.session = session

        # Reinstantiate Tables
        base.metadata.drop_all(db)
        base.metadata.create_all(db)

        # Call Bulk upload data 
        self.load_bulk_people_data()
        self.load_bulk_company_data()
        self.load_bulk_li_mapping_data()
    
    # Bulk upload people data.
    def load_bulk_people_data(self):
        
        people_path = os.getenv('PEOPLE_BULK_DATA')
        people = list(utils.iter_people_from_file(people_path))
        
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
            self.session.add(person)  
        self.session.commit()

    # Bulk upload company data.
    def load_bulk_company_data(self): 

        companies_path = os.getenv('COMPANIES_BULK_DATA')
        companies = list(utils.iter_companies_from_file(companies_path))

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
            self.session.add(company)  
        self.session.commit()
    
    # Bulk upload company mapping data.
    def load_bulk_li_mapping_data(self): 
        
        companies_path = os.getenv('COMPANIES_BULK_DATA')
        company_li_mappings = list(utils.iter_company_li_from_file(companies_path))

        for company_li_data in company_li_mappings: 
            company_li_name = company_li_data['company_li_name']
            company_name = utils.parse_strings(company_li_data['company_name'])

            company_li_mapping = Company_Li_Mapping(
                company_name = company_name,
                company_li_name = company_li_name
            )
            self.session.add(company_li_mapping)
        self.session.commit()
    
    # # Read
    # people = session.query(Person)  
    # for person in people:  
    #     print(person.person_id)


loader = Loader() 
loader.load_data()

