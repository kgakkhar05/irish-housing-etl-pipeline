# -----------------------------
# 1. IMPORT LIBRARIES
# -----------------------------

import requests
import os
import mysql.connector

# -----------------------------
# 2. EXTRACT - CSO Ireland API
# -----------------------------

url_api = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/HPA13/JSON-stat/2.0/en"
response_api = requests.get(url_api, timeout=30)

# Raise an error if the API request was unsuccessful
response_api.raise_for_status()

data_api = response_api.json()
print("HPA13 API extraction successful.")

print(type(data_api))
print(data_api)
print(data_api.keys())
print(data_api["id"])
print(data_api["dimension"])
print(data_api["dimension"]["STATISTIC"]["category"]["label"])
print(data_api["dimension"]["TLIST(A1)"]["category"]["label"])
print(data_api["dimension"]["C02803V03373"]["category"]["label"])
print(data_api["value"][:10])


# -----------------------------
# 3. TRANSFORM - RESHAPE API DATA
# -----------------------------

print(data_api["size"])
print(len(data_api["value"]))

#Assigning values to variables
# Source dimension codes and labels from the API data
statistics = data_api["dimension"]["STATISTIC"]["category"]["index"]
statistic_labels = data_api["dimension"]["STATISTIC"]["category"]["label"]
years = data_api["dimension"]["TLIST(A1)"]["category"]["index"]
property_types = data_api["dimension"]["C02803V03373"]["category"]["index"]
property_type_labels = data_api["dimension"]["C02803V03373"]["category"]["label"]

# Flat list of API values
values = data_api["value"]

# Convert the JSON-stat structure into database-friendly rows
rows = []
index = 0
for statistic in statistics:
    for year in years:
        for property_type in property_types:
            rows.append({
                "statistic_code": statistic,
                "statistic_label": statistic_labels[statistic],
                "year": year,
                "property_type": property_type,
                "property_type_label": property_type_labels[property_type],
                "value": values[index]
            })
            index += 1

# f - They allow variables and expressions to be directly embedded inside strings using curly braces {}.
print(f"HPA13 rows prepared: {len(rows)}")


# -----------------------------
# 4. CONNECT TO MYSQL
# -----------------------------

# Read the MySQL password from an environment variable
mysql_password = os.getenv("MYSQL_PASSWORD")
if not mysql_password:
    raise ValueError(
        "MYSQL_PASSWORD environment variable has not been set."
    )

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password=mysql_password
)


# Create cursor for executing SQL commands
cursor = connection.cursor()
print("Connected to MySQL database.")

# -----------------------------
# 5. CREATE / SELECT DATABASE
# -----------------------------

# Create database if it does not exist
cursor.execute("CREATE DATABASE IF NOT EXISTS airflow_housing")

# Select the project database
connection.database = "airflow_housing"

# -----------------------------
# 6. CREATE RAW / STAGING TABLE
# -----------------------------

# Create the table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS raw_hpa13_data (
    statistic_code VARCHAR(20),
    statistic_label VARCHAR(255),
    year VARCHAR(4),
    property_type VARCHAR(50),
    property_type_label VARCHAR(400),
    value DECIMAL(15,2),
    UNIQUE KEY unique_hpa13_row (statistic_code, year, property_type)
)
""")

# -----------------------------
# 7. LOAD DATA INTO MYSQL
# -----------------------------

# Insert the data into the table
insert_sql = """
INSERT IGNORE INTO raw_hpa13_data(
    statistic_code, 
    statistic_label, 
    year, 
    property_type, 
    property_type_label, 
    value
    )
VALUES (%s, %s, %s, %s, %s, %s)
"""
insert_values =[
(
    row["statistic_code"],
    row["statistic_label"],
    row["year"],
    row["property_type"],
    row["property_type_label"],
    row["value"]
)
    for row in rows
]
cursor.executemany(insert_sql, insert_values)
connection.commit()

print(
    f"HPA13 load complete. "
    f"{len(insert_values)} rows processed."
)

# -----------------------------
# 8. CLOSE MYSQL CONNECTION
# -----------------------------

cursor.close()
connection.close()

print("MySQL connection closed.")