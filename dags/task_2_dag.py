import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
import logging

@dag(
    start_date=datetime(2022, 5, 25),
    schedule_interval="00 00 * * *",
    catchup=False,
    default_args={"owner": "vytautas", "retries": 3},
    tags=["task_2"],
    max_active_tasks=1,
)
def task_2():
    @task()
    def process_csv(ds=None, **kwargs):
        csv_file = f"/usr/local/airflow/output/{ds}.csv"
        table_name = "csv_data_task"
        postgres_conn_id = "postgres_data"

        logging.info(f"Processing CSV file: {csv_file}")
        try:
            # Load CSV file into a pandas DataFrame
            df = pd.read_csv(csv_file)
            logging.info(f"CSV file loaded successfully. Shape: {df.shape}")

            # Validate required columns
            required_columns = {"date", "hour", "impression_count", "click_count"}
            if not required_columns.issubset(df.columns):
                raise ValueError(
                    f"CSV is missing required columns. Found: {df.columns}, Expected: {required_columns}"
                )

            # Transform the data
            df["datetime"] = pd.to_datetime(df["date"] + " " + df["hour"].astype(str) + ":00:00")
            df = df[["datetime", "impression_count", "click_count"]]
            df["audit_loaded_datetime"] = datetime.now()

            # Initialize Postgres connection
            pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            engine = pg_hook.get_sqlalchemy_engine()

            # Create table if it doesn't exist
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    datetime TIMESTAMP NOT NULL UNIQUE,
                    impression_count BIGINT NOT NULL,
                    click_count BIGINT NOT NULL,
                    audit_loaded_datetime TIMESTAMP NOT NULL
                )
            """
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
                    logging.info(f"Table {table_name} verified or created successfully.")

            # Insert data into the table using pandas
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=25,
            )
            logging.info(f"Data successfully inserted into {table_name}.")

        except FileNotFoundError as e:
            logging.error(f"File not found: {csv_file}. Exception: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    process_csv()

# Instantiate the DAG
task_2()
