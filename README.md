# Irish Housing ETL Pipeline

A personal end-to-end data engineering and analytics project using Irish CSO housing data.

The project is designed to strengthen backend data skills across Python, SQL, MySQL, pandas, Git/GitHub, and Apache Airflow, with Tableau used as the downstream reporting layer.

## Project Architecture

CSO APIs  
↓  
Python extraction and reshaping  
↓  
MySQL raw/staging tables  
↓  
SQL transformations and joins  
↓  
Reporting view / stored procedure  
↓  
Physical reporting table  
↓  
pandas validation  
↓  
Airflow orchestration  
↓  
Tableau

## Data Sources

Two CSO Ireland housing datasets are used:

- HPA13 — Residential Property Price Index
- HPA05 — Residential property sales statistics

## Current Pipeline

### Python ETL

Python scripts:

- extract data from CSO REST APIs
- parse JSON-stat responses
- reshape multidimensional API data into relational rows
- load data into MySQL staging tables

Raw/staging tables:

- `raw_hpa13_data`
- `raw_hpa05_data`

### SQL Transformation

SQL logic is used to:

- map HPA13 region and dwelling-type values to HPA05
- pivot HPA13 statistics into separate measures
- join HPA05 and HPA13
- validate row counts and unmatched records

A reusable reporting view was created:

- `vw_housing_reporting`

### Stored Procedure / ELT

A physical reporting table was created:

- `reporting_housing`

The stored procedure:

- `sp_refresh_housing_reporting`

refreshes the reporting table from the staging tables.

The resulting reporting dataset contains 48,384 rows.

### pandas Validation

pandas is used to validate the reporting dataset, including:

- row and column counts
- data types
- null values
- duplicate checks
- missing values by statistic
- missing values by region
- missing values by year

## Technologies

- Python
- pandas
- MySQL
- SQL
- SQLAlchemy
- REST APIs
- JSON-stat
- Git
- GitHub
- Apache Airflow
- Tableau

## Project Status

Completed:

- API extraction
- Python data reshaping
- MySQL staging tables
- SQL joins and transformations
- reporting view
- pandas data validation
- stored procedure
- physical reporting table
- Git/GitHub version control

Next:

- refactor ETL scripts into reusable functions
- build Airflow DAG
- add scheduling, dependencies and retries
- connect final reporting output to Tableau