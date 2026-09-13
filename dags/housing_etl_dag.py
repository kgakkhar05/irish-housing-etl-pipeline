# -----------------------------
# 1. IMPORTS
# -----------------------------

from airflow.sdk import dag, task
from datetime import datetime

import sys
from pathlib import Path

from datetime import datetime, timedelta

# -----------------------------
# 2. PROJECT PATH SETUP
# -----------------------------

# Add the project root folder to Python's import path
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# -----------------------------
# 3. DEFINE DAG
# -----------------------------

@dag(
    dag_id="housing_etl_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule="0 7 1 * *",  #minute = 0, hour = 7, day = every day, 1st of each month, month = every month, weekday = every day
    catchup=False,
    default_args=default_args,
    tags=["housing", "etl"]
)
def housing_etl_pipeline():


    # -----------------------------
    # TASK 1 - HPA13 ETL
    # -----------------------------

    @task
    def run_hpa13_etl():

        from scripts.hpa13_etl import main

        main()


    # -----------------------------
    # TASK 2 - HPA05 ETL
    # -----------------------------

    @task
    def run_hpa05_etl():

        from scripts.hpa05_etl import main

        main()


    # -----------------------------
    # TASK 3 - REFRESH REPORTING TABLE
    # -----------------------------

    @task
    def refresh_reporting_table():

        import os
        import mysql.connector

        mysql_password = os.getenv("MYSQL_PASSWORD")

        if not mysql_password:
            raise ValueError(
                "MYSQL_PASSWORD environment variable has not been set."
            )

        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password=mysql_password,
            database="airflow_housing"
        )

        cursor = connection.cursor()

        # Run the existing MySQL stored procedure
        cursor.callproc("sp_refresh_housing_reporting")

        connection.commit()

        cursor.close()
        connection.close()

        print("Reporting table refreshed successfully.")

    # -----------------------------
    # TASK 4 - VALIDATE REPORTING TABLE
    # -----------------------------

    @task
    def validate_reporting_table():

        import os
        import mysql.connector

        mysql_password = os.getenv("MYSQL_PASSWORD")

        if not mysql_password:
            raise ValueError(
                "MYSQL_PASSWORD environment variable has not been set."
            )

        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password=mysql_password,
            database="airflow_housing"
        )

        cursor = connection.cursor()

        cursor.execute("""
            SELECT COUNT(*)
            FROM reporting_housing
        """)

        row_count = cursor.fetchone()[0]

        cursor.close()
        connection.close()

        print(f"Reporting table row count: {row_count}")

        if row_count == 0:
            raise ValueError(
                "Validation failed: reporting_housing contains no rows."
            )

        print("Reporting table validation successful.")

    # -----------------------------
    # 4. CREATE TASK INSTANCES
    # -----------------------------

    hpa13_task = run_hpa13_etl()

    hpa05_task = run_hpa05_etl()

    refresh_task = refresh_reporting_table()

    validation_task = validate_reporting_table()

    # -----------------------------
    # 5. DEFINE TASK DEPENDENCIES
    # -----------------------------

    [hpa13_task, hpa05_task] >> refresh_task >> validation_task


# -----------------------------
# 6. CREATE DAG
# -----------------------------

housing_etl_pipeline()