-- Problem 1. Finding the average known total funding across companies a person has worked at.
SELECT
	avg(companies.known_total_funding)
FROM
	people
	INNER JOIN company_li_names ON people.company_li_name = company_li_names.company_li_name
	INNER JOIN companies ON company_li_names.company_name = companies.company_name
WHERE (companies.company_name = people.company_name
	OR company_li_names.company_li_name = people.company_li_name)
AND person_id = '92a52877-8d5d-41a6-950f-1b9c6574be7a'

-- Answer 1: 108000000.000000000000

-- Problem 2. Finding the number of companies in the companies table where no person has worked
SELECT
	count(company_name)
FROM
	companies
WHERE
	company_name NOT IN(
		SELECT
			company_name FROM people
		WHERE
			company_name IS NOT NULL)

--Answer 2: 9390

-- Problem 3. Top 10 companies based on headcount. 
select company_name from people 
group by company_name
order by count(company_name) DESC
limit 10

-- Answer 3: 
-- microsoft  
-- amazon
-- intel corporation
-- google
-- apple
-- hewlett packard enterprise 
-- facebook
-- texas-instruments
-- hewlett packard
-- meta

SELECT
	people.person_id,
	companies.headcount,
	people.company_name,
FROM
	people
INNER JOIN company_li_names ON people.company_li_name = company_li_names.company_li_name
INNER JOIN companies on people.company_name = companies.company_name or company_li_names.company_li_name = companies.company_name 
WHERE
	people.is_founder = TRUE and headcount is not NULL
ORDER BY companies.headcount DESC
limit 3

-- Answer 4: 
-- 1. Person ID: bb0d8489-4360-4a94-bd3d-c079f75afc96	Headcount: 2907	    Company: dafiti
-- 2. Person ID: a292842c-475e-4b4f-9671-fb09536c472e	Headcount: 1336	    Company: ebay for business
-- 3. Person ID: c6f69f63-c7d5-419f-af34-d0cccf544e18	Headcount: 439	    Copmany: uworld	

-- Problem 5. Identify the second most recent job for a person, find the average duration across all second jobs in that role 

SELECT
	avg(extract(years FROM group_end_date) - extract(years FROM group_start_date))
FROM 
	SELECT
		group_start_date,
		group_end_date,
		person_id,
		ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY people.group_start_date DESC)
	FROM
		people
WHERE
	ROW_NUMBER = 2

-- Answer 5: Average duration in the second role: 3.3805309734513274

-- Problem 5b. How many people have 1 > one job? 
 
 SELECT
	people.person_id
FROM
	people
WHERE (
	SELECT
		count(*)
	FROM
		people p
	WHERE
		people.person_id = p.person_id) > 1
GROUP BY
	people.person_id

--Answer 5b. 904 people.
