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

def iter_people_from_file(path: str) -> Iterator[Dict[str, Any]]:
    skip = True 
    with open(path, 'r') as f:
        csv_reader = csv.reader(f)
        for line in csv_reader:
            if skip: # skip the headers
                skip = False 
                continue
            person = {}
            person['person_id'] =           line[0]
            person['company_name'] =        line[1]
            person['company_li_name'] =     line[2]
            person['last_title'] =          line[3]
            person['group_start_date'] =    line[4]
            person['group_end_date'] =      line[5]

            yield person

#------------------------ Create

def create_staging_table(cursor):
    cursor.execute("""
        DROP TABLE IF EXISTS people;
        CREATE TABLE people (
            person_id           UUID,
            company_name        TEXT,
            company_li_name     TEXT,
            last_title          TEXT,
            is_founder          BOOL,
            group_start_date    DATE,
            group_end_date      DATE
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

def parse_title_for_founder(text: str) -> bool:
    text = text.lower()
    if re.search('founder', text): 
        return True
    else:
        return False

def parse_linked_li_name(text: str) -> bool:
    # clean up data when n/a provided by person
    if re.search('n/a', text): 
        return None
    else:
        return text

def parse_currently_at(date: str):
    # set currently works at company when person does not provide end date
    end_date = parse_date(date)
    if end_date: 
        return end_date
    else:
        return datetime.date.today()

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
@profile
def insert_execute_batch_iterator(connection, people: Iterator[Dict[str, Any]]) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)
        
        iter_people = ({
            **person,
            'company_name':person['company_name'].lower(),
            'company_li_name': parse_linked_li_name(person['company_li_name']),
            'last_title': person['last_title'].lower(),
            'is_founder': parse_title_for_founder(person['last_title']),
            'group_start_date': parse_date(person['group_start_date']),
            'group_end_date': parse_currently_at(person['group_end_date']),
        } for person in people)

        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO people VALUES (
                %(person_id)s,
                %(company_name)s,
                %(company_li_name)s,
                %(last_title)s,
                %(is_founder)s,
                %(group_start_date)s,
                %(group_end_date)s
            );
        """, iter_people)

path = '/Users/poojakale/Documents/interviews/people.csv'
people = list(iter_people_from_file(path))
insert_execute_batch_iterator(connection, people)
