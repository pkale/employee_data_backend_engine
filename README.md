# employee_data_backend_engine

# Project Summary

We are building a tier 1 service to clense, ingest, store and query data representing people's employment history and company data. The data lives locally in a relational database (PostgresSQL)The ingestion and querying is carrier out via an ORM (SQL Alchemy). This provides The data is lighly clensed to make relational mapping more accurate between the tables and columns (Logic can be found in load script). The backend app is built using Flask with intentions to make it easily extensible to a front end framework in the future. 

## Understanding key points about the data: 

- All the companies people worked for are not in the companies table 
- There are some companies that have no people data stored 
- Some companies have multiple entries in the companies table
- The company name people provide might not match the name in company data. There are companies that match the people data based on company name and company linkedin. 
- There is a one to many mapping between company linkedin names and companies names in the companies table.

## Assumptions we will make: 
- If people did not provide an end date, we assume they work there.
- If the title people provide includes the substring founder, they are a founder at the company 
- Companies can be referenced accurately by people using company name or company linkedin


## Set Up Guide 

1. Dowload project and set up on your local machine. 
2. Set up virtual enviorment with python 3.8 and above 
3. cd into the main project folder and run the following command to install all dependencies.
    ```
    pip install -r requirments.txt
    ```
4. Once all the required packages are installed, launch the app by running the command below
    ```
    flask run
    ```
    When the app is run, the data is automatically loaded onto the database. The data can be accessed at the following URL: 
    ```
    postgresql://admin:admin@localhost:5432/poojakale
    ```
5. There are 3 APIs defined as follows: 
    ```
    /average-funding-by-person/[person_id]
    /companies-by-person/[person_id]
    /investors-by-company/[company_li_name]
    ```

