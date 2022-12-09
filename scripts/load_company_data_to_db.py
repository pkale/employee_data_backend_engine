from typing import Iterator, Dict, Any, Optional
import psycopg2, psycopg2.extras
import time, datetime
import re
from functools import wraps
import csv
from memory_profiler import memory_usage


#------------------------ Connect

connection = psycopg2.connect(
    host="localhost",
    port=5432,
    database="poojakale",
    user="admin",
    password="admin",
)
connection.set_session(autocommit=True)

#------------------------ Read

def iter_company_from_file(path: str) -> Iterator[Dict[str, Any]]:
    skip = True 
    idx = 0
    with open(path, 'r') as f:
        csv_reader = csv.reader(f)
        for line in csv_reader:
            if skip: # skip the headers
                skip = False 
                continue
            company = {}
            company['id'] = idx
            company['company_name'] =            line[0]
            company['company_li_names'] =        line[1]
            company['description'] =             line[2]
            company['headcount'] =               line[3]
            company['founding_date'] =           line[4]
            company['most_recent_raise'] =       line[5]
            company['most_recent_valuation'] =   line[6]
            company['investors'] =               line[7]
            company['known_total_funding'] =     line[8]
            idx +=1
            yield company
    
def iter_company_li_from_file(path: str) -> Iterator[Dict[str, Any]]:
    skip = True 
    idx = 0
    with open(path, 'r') as f:
        csv_reader = csv.reader(f)
        for line in csv_reader:
            if skip: # skip the headers
                skip = False 
                continue
            company_li_names = line[1].replace('\n','').replace('"','').replace(' ','').replace('[','').replace(']','').split(',')
            if company_li_names == ['']: company_li_names = []
            for li_name in company_li_names: 
                company = {}
                company['id'] = idx
                company['company_name'] =            line[0]
                company['company_li_name'] =         li_name.strip()
                print(company)
                idx +=1
                yield company

#------------------------ Analyze Load Metrics

def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        print('Load Metrics: \n')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time   {elapsed:0.4}')

        # Measure memory
        mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

        print(f'Memory {max(mem) - min(mem)}')
        return retval

    return inner
    
#------------------------ Load

def create_companies_staging_table(cursor):
    cursor.execute("""
        DROP TABLE IF EXISTS companies;
        CREATE TABLE companies (
            id                      NUMERIC,
            company_name            TEXT,
            company_li_names        TEXT,
            description             TEXT,
            headcount               NUMERIC,
            founding_date           DATE,
            most_recent_raise       NUMERIC,
            most_recent_valuation   NUMERIC,
            investors               TEXT,
            known_total_funding     NUMERIC
        );
    """)

def create_company_li_names_table(cursor):
    cursor.execute("""
        DROP TABLE IF EXISTS company_li_names;
        CREATE TABLE company_li_names (
            id                       NUMERIC,
            company_li_name          TEXT,
            company_name             TEXT
        );
    """)

def parse_date(text: str) -> datetime.date:
    
    if text == '': 
        return None # replace empty string with None
    parts = text.split('-')
    if len(parts) == 3:
        return datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
    else:
        print(parts)
        assert False, 'Unknown date format'

def parse_numeric_value(value: str) -> int:
    # set currently works at company when person does not provide end date
    if str.isalpha(value) or value == '': 
        return None
    else:
        return int(value)

def parse_lists(text: str) -> str: 

    if text == '[]' or text == '': 
        return None 
    else: 
        return text.lower()

@profile
def clean_db_and_insert_execute_batch_iterator(connection, companies: Iterator[Dict[str, Any]], page_size: int = 1) -> None:
    with connection.cursor() as cursor:
        create_companies_staging_table(cursor)
        
        iter_companies = ({
            **company,
            'company_name':company['company_name'].lower(),
            'company_li_names': parse_lists(company['company_li_names']), # psycopg converts str to list
            'founding_date': parse_date(company['founding_date']),
            'headcount':parse_numeric_value(company['headcount']),
            'most_recent_raise':parse_numeric_value(company['most_recent_raise']),
            'most_recent_valuation':parse_numeric_value(company['most_recent_valuation']),
            'investors': parse_lists(company['investors']),          
            'known_total_funding':parse_numeric_value(company['known_total_funding']),
        } for company in companies)

        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO companies VALUES (
                %(id)s,
                %(company_name)s,
                %(company_li_names)s,
                %(description)s,
                %(headcount)s,
                %(founding_date)s,
                %(most_recent_raise)s,
                %(most_recent_valuation)s,
                %(investors)s,
                %(known_total_funding)s
            );
        """, iter_companies, page_size=page_size)

@profile
def insert_execute_batch_iterator(connection, companies: Iterator[Dict[str, Any]], page_size: int = 1) -> None:
    with connection.cursor() as cursor:        
        iter_companies = ({
            **company,
            'company_name':company['company_name'].lower(),
            'company_li_names': parse_lists(company['company_li_names']), # psycopg converts str to list
            'founding_date': parse_date(company['founding_date']),
            'headcount':parse_numeric_value(company['headcount']),
            'most_recent_raise':parse_numeric_value(company['most_recent_raise']),
            'most_recent_valuation':parse_numeric_value(company['most_recent_valuation']),
            'investors': parse_lists(company['investors']),            
            'known_total_funding':parse_numeric_value(company['known_total_funding']),
        } for company in companies)

        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO companies VALUES (
                %(id)s,
                %(company_name)s,
                %(company_li_names)s,
                %(description)s,
                %(headcount)s,
                %(founding_date)s,
                %(most_recent_raise)s,
                %(most_recent_valuation)s,
                %(investors)s,
                %(known_total_funding)s
            );
        """, iter_companies, page_size=page_size)

def wipe_and_insert_execute(connection, company_li_names: Iterator[Dict[str,any]]): 
    with connection.cursor() as cursor: 
        create_company_li_names_table(cursor)

        psycopg2.extras.execute_values(cursor, """ INSERT INTO company_li_names VALUES %s; """,
        (
            (
            company['id'],
            company['company_li_name'],
            company['company_name'].strip().lower(),
            ) for company in company_li_names)
        )


path = '/Users/poojakale/Documents/interviews/companies.csv'
companies = list(iter_company_from_file(path))
clean_db_and_insert_execute_batch_iterator(connection, companies, page_size=100)

company_li_names = list(iter_company_li_from_file(path))
wipe_and_insert_execute(connection,company_li_names)