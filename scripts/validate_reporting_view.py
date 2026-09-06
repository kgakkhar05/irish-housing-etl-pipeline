# -------------------------
# 1. IMPORT LIBRARIES
# -------------------------

# pandas is used for loading and validating the reporting dataset
import os
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# mysql.connector allows Python to connect to the local MySQL database
# import mysql.connector -  We commented this out because we are using SQLAlchemy to connect to MySQL instead of mysql.connector

""" # -------------------------
# 2. CONNECT TO MYSQL
# -------------------------

# Connect to the local MySQL database containing the housing data
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password=os.getenv("MYSQL_PASSWORD"),
    database="airflow_housing"
)  """

# -------------------------
# 2. CREATE MYSQL CONNECTION
# -------------------------

# Read the MySQL password from an environment variable
mysql_password = os.getenv("MYSQL_PASSWORD")

# Build the MySQL connection URL
connection_url = URL.create(
    "mysql+pymysql",
    username="root",
    password=mysql_password,
    host="localhost",
    database="airflow_housing"
)

# Create a SQLAlchemy engine for connecting pandas to MySQL
engine = create_engine(connection_url)

# -------------------------
# 3. EXTRACT REPORTING VIEW
# -------------------------

# raw_hpa05_data and raw_hpa13_data act as the raw/staging layer.
# vw_housing_reporting is the transformed reporting layer used for analysis.

query = """
SELECT *
FROM vw_housing_reporting
"""

# -------------------------
# 4. LOAD DATA INTO PANDAS
# -------------------------

# Read the MySQL reporting view into a pandas DataFrame.
# A DataFrame is a table-like structure in Python.
df=pd.read_sql_query(query, engine)

# -------------------------
# 5. BASIC DATA VALIDATION
# -------------------------

# Display the first five rows to inspect the structure of the dataset
print("Data loaded into pandas DataFrame:")
print(df.head())

# Display the number of rows and columns
print("\nNumber of rows and columns:") #\n for new line in output
print(df.shape)

# -------------------------
# 6. DATA QUALITY CHECKS
# -------------------------

print("\nChecking data types:")
print(df.dtypes)

print("\nChecking for missing/null values:")
print(df.isnull().sum())

print("\nChecking for duplicate rows:")
print(df.duplicated().sum())

# -------------------------
# 7. INSPECT MISSING HPA05 VALUES
# -------------------------

# Display rows where the main HPA05 value is missing
missing_hpa05 = df[
    df['hpa05_value'].isnull()
]

print("\nRows with missing HPA05 values:")
print(missing_hpa05.head(298))  # Display the rows with missing HPA05 values

""" print("\nNumber of rows with missing HPA05 values:")
print(len(missing_hpa05)) """ # Not needed since we can see the number of rows in the output of the above print statement

# -------------------------
# 8. ANALYSE MISSING VALUES BY STATISTIC
# -------------------------

# Count missing HPA05 values for each statistic
# Size - count how many rows are in each group, pandas usually stores the grouping fields as an index, rather than normal columns.
# reset_index() → turns the grouped fields back into normal columns, This is like COUNT(*) in SQL
# name="missing_count" → gives the count column a readable name, This is like AS missing_count in SQL

missing_by_statistic = missing_hpa05.groupby(['hpa05_statistic_code', 'hpa05_statistic_label']).size().reset_index(name='missing_count')
print("\nMissing HPA05 values by statistic:")
print(missing_by_statistic)

# -------------------------
# 9. COMPARE TOTAL VS MISSING VALUES BY STATISTIC
# -------------------------

# Count total rows for each HPA05 statistic
total_by_statistic = (
    df
    .groupby(
        ["hpa05_statistic_code", "hpa05_statistic_label"]
    )
    .size()
    .reset_index(name="total_rows")
)

# Merge the total counts with the missing-value counts
missing_summary = total_by_statistic.merge(
    missing_by_statistic,
    on=["hpa05_statistic_code", "hpa05_statistic_label"],
    how="left"
)

# Statistics with no missing values will have NaN after the merge,
# so replace those with 0
missing_summary["missing_count"] = (
    missing_summary["missing_count"]
    .fillna(0)
    .astype(int)
)

# Calculate the percentage of rows with missing HPA05 values
missing_summary["missing_percentage"] = (
    missing_summary["missing_count"]
    / missing_summary["total_rows"]
    * 100
)

print("\nMissing-value summary by statistic:")
print(missing_summary)

# Missing hpa05_value records occur only within the Mean Sale Price and Median Price statistics, with 149 missing rows in each. 
# The statistics themselves are not mostly or entirely null.

# -------------------------
# 10. ANALYSE WHERE HPA05 VALUES ARE MISSING
# -------------------------

missing_by_region = missing_hpa05.groupby(['region', 'region_label']).size().reset_index(name='missing_count')
print("\nMissing HPA05 values by region:")
print(missing_by_region)

# -------------------------
# 11. ANALYSE MISSING VALUES BY YEAR
# -------------------------

missing_by_year = missing_hpa05.groupby(['year']).size().reset_index(name='missing_count')
print("\nMissing HPA05 values by year:")
print(missing_by_year)