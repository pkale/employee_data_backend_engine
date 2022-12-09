from typing import Iterator, Dict, Any
from datetime import date
import csv, re

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

def iter_companies_from_file(path: str) -> Iterator[Dict[str, Any]]:
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
                company['company_name'] = line[0]
                company['company_li_name'] = li_name.strip()
                idx +=1
                yield company

def parse_li_name(text: str) -> bool:
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

def parse_date(text: str) -> date:
    if text == '': 
        return None # replace empty string with None
    parts = text.split('-')
    if len(parts) == 3:
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    else:
        print(parts)
        assert False, 'Unknown date format'

def parse_end_date(text: str):
    # set currently works at company when person does not provide end date
    end_date = parse_date(text)
    if end_date: 
        return end_date
    else:
        return date.today()

def parse_title_for_founder(text: str) -> bool:
    text = text.lower()
    if re.search('founder', text): 
        return True
    else:
        return False

def parse_lists(text: str) -> str: 

    if text == '[]' or text == '': 
        return None 
    else: 
        return text.lower()

def parse_strings(text: str) -> str: 

    return text.lower().strip()

def parse_numeric_value(value: str) -> int:
    # set currently works at company when person does not provide end date
    if str.isalpha(value) or value == '': 
        return None
    else:
        return int(value)