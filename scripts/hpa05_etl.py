# -----------------------------
# 1. IMPORT LIBRARIES
# -----------------------------

import requests
import os
import mysql.connector

# -----------------------------
# 2. EXTRACT - CSO Ireland API
# -----------------------------

def extract_hpa05():
    url_api = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/HPA05/JSON-stat/2.0/en"
    response_api = requests.get(url_api, timeout=30)

    # Raise an error if the API request was unsuccessful
    response_api.raise_for_status()

    print(response_api.status_code)
    print("HPA05 API extraction successful.")

    data_api = response_api.json() #Inspect HPA05 structure
    # print(type(data_api)) 
    # print(data_api.keys())
    # print(data_api["id"])
    # for dimension_id in data_api["id"]:
    #     print(dimension_id, "->", data_api["dimension"][dimension_id]["label"])

    # print(data_api["dimension"]["C03347V04034"]["category"]["label"]) 
    # print(data_api["dimension"]["C03346V04033"]["category"]["label"]) 

    # print(data_api["dimension"]["C03341V04028"]["category"]["label"]) 

    # for dimension_id_test in data_api["id"]:
    #     print(dimension_id_test, "->", data_api["dimension"][dimension_id_test]["category"]["label"])
    return data_api 

# -----------------------------
# 3. TRANSFORM - RESHAPE API DATA
# -----------------------------

def transform_hpa05(data_api):
    print(data_api["size"])
    print(len(data_api["value"]))

    #Assigning values to variables
    statistics = data_api["dimension"]["STATISTIC"]["category"]["index"]
    years = data_api["dimension"]["TLIST(A1)"]["category"]["index"]
    dwelling_types = data_api["dimension"]["C03347V04034"]["category"]["index"]
    dwelling_statuses = data_api["dimension"]["C03346V04033"]["category"]["index"]
    stamp_duty_events = data_api["dimension"]["C03341V04028"]["category"]["index"]
    regions = data_api["dimension"]["C03348V04035"]["category"]["index"]
    values = data_api["value"]

    #Add labels for each dimension
    statistic_labels = data_api["dimension"]["STATISTIC"]["category"]["label"]
    dwelling_type_labels = data_api["dimension"]["C03347V04034"]["category"]["label"]
    dwelling_status_labels = data_api["dimension"]["C03346V04033"]["category"]["label"]
    stamp_duty_event_labels = data_api["dimension"]["C03341V04028"]["category"]["label"]
    region_labels = data_api["dimension"]["C03348V04035"]["category"]["label"]



    rows = []
    index = 0
    for statistic in statistics:
        for year in years:
            for dwelling_type in dwelling_types:
                for dwelling_status in dwelling_statuses:
                    for stamp_duty_event in stamp_duty_events:
                        for region in regions:
                            rows.append({
                                "statistic_code": statistic,
                                "statistic_label": statistic_labels[statistic],
                                "year": year,
                                "dwelling_type": dwelling_type,
                                "dwelling_type_label": dwelling_type_labels[dwelling_type],
                                "dwelling_status": dwelling_status,
                                "dwelling_status_label": dwelling_status_labels[dwelling_status],
                                "stamp_duty_event": stamp_duty_event,
                                "stamp_duty_event_label": stamp_duty_event_labels[stamp_duty_event],
                                "region": region,
                                "region_label": region_labels[region],
                                "value": values[index]
                            })
                            index += 1

    print(f"HPA05 rows prepared: {len(rows)}")
    return rows


# -----------------------------
# 4. CONNECT TO MYSQL
# -----------------------------

def load_hpa05(rows):
    # Read the MySQL password from an environment variable
    mysql_password = os.getenv("MYSQL_PASSWORD")
    if not mysql_password:
        raise ValueError(
            "MYSQL_PASSWORD environment variable has not been set."
        )

    # Connect to MySQL database
    connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password=mysql_password
    )
    print(connection.is_connected())

    # Create cursor for executing SQL commands
    cursor = connection.cursor()
    print("Connected to MySQL database.")

    # -----------------------------
    # 5. CREATE / SELECT DATABASE
    # -----------------------------

    # Create database if it does not exist - additionally added, this ensures that one script is not dependent on another merely for database creation.
    cursor.execute("CREATE DATABASE IF NOT EXISTS airflow_housing")

    # Select the project database
    connection.database = "airflow_housing"

    # -----------------------------
    # 6. CREATE RAW / STAGING TABLE
    # -----------------------------

    # Create the table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_hpa05_data (
        statistic_code VARCHAR(20),
        statistic_label VARCHAR(300),
        year VARCHAR(10),
        dwelling_type VARCHAR(20),
        dwelling_type_label VARCHAR(300),
        dwelling_status VARCHAR(20),
        dwelling_status_label VARCHAR(300),
        stamp_duty_event VARCHAR(20),
        stamp_duty_event_label VARCHAR(300),
        region VARCHAR(20),
        region_label VARCHAR(300),
        value DECIMAL(15,2),
        UNIQUE KEY unique_hpa05_row (
            statistic_code, 
            year, 
            dwelling_type, 
            dwelling_status, 
            stamp_duty_event, 
            region
        )
    )
    """)

    # -----------------------------
    # 7. LOAD DATA INTO MYSQL
    # -----------------------------

    # Insert the data into the table
    insert_sql = """
    INSERT IGNORE INTO raw_hpa05_data (
        statistic_code, 
        statistic_label, 
        year,
        dwelling_type,
        dwelling_type_label,
        dwelling_status,
        dwelling_status_label,
        stamp_duty_event,
        stamp_duty_event_label,
        region,
        region_label,
        value
        )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_values =[
    (
        row["statistic_code"],
        row["statistic_label"],
        row["year"],
        row["dwelling_type"],
        row["dwelling_type_label"],
        row["dwelling_status"],
        row["dwelling_status_label"],
        row["stamp_duty_event"],
        row["stamp_duty_event_label"],
        row["region"],
        row["region_label"],
        row["value"]
    )
        for row in rows
    ]

    cursor.executemany(insert_sql, insert_values)
    connection.commit()

    print(
        f"HPA05 load complete. "
        f"{len(insert_values)} rows processed."
    )

    # -----------------------------
    # 8. CLOSE MYSQL CONNECTION
    # -----------------------------

    cursor.close()
    connection.close()

    print("MySQL connection closed.")

# -----------------------------
# 9. MAIN PIPELINE
# -----------------------------

def main():

    data_api = extract_hpa05()

    rows = transform_hpa05(data_api)

    load_hpa05(rows)


# -----------------------------
# 10. RUN SCRIPT
# -----------------------------

if __name__ == "__main__":
    main()