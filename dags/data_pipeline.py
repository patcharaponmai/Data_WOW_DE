from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from etl_function import ETL

SOURCE_FILE_PATH = './data_sample/data_test' # For run all file change this path to './data_sample'
TARGET_TABLE = 'test_tbl'
COLUMNS = ['department_name', 'sensor_serial', 'create_at', 'product_name', 'product_expire']
POSTGRES_CONN_ID = 'postgres_default'


etl_instance = ETL(source_file_path=SOURCE_FILE_PATH, connection=POSTGRES_CONN_ID, tgt_table=TARGET_TABLE)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry': 10,
}

dag = DAG(
    'ETL_process',
    default_args=default_args,
    description='ETL pipeline',
    schedule_interval=None,
    concurrency=30
)

# Main function to execute ETL process
def etl_pipeline():
    etl_instance.extract_data()
    etl_instance.transform_data()
    etl_instance.load_data()

# Explore data after etl process complete
def explore_data():
    # Create a PostgresHook to execute the SQL query
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = f"SELECT * FROM {TARGET_TABLE} LIMIT 10"
    
    # Execute the query and fetch the results
    result = pg_hook.get_records(sql)
    
    # Convert result into DataFrame
    df = pd.DataFrame(result, columns=COLUMNS) 
    print(df)

drop_table = PostgresOperator(
    task_id="drop_table",
    sql=f"DROP TABLE IF EXISTS {TARGET_TABLE} CASCADE",
    dag=dag
)

create_table_query=f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    department_name VARCHAR(32), -- Data type length refer to function gen_text() in sampledata_new.py line 33
    sensor_serial VARCHAR(64), -- Data type length refer to function gen_text() in sampledata_new.py line 34
    create_at TIMESTAMP,
    product_name VARCHAR(16), -- Data type length refer to function gen_text() in sampledata_new.py line 35
    product_expire TIMESTAMP)
    PARTITION BY RANGE (create_at);

    -- Create child tables for specific timestamp ranges
    CREATE TABLE {TARGET_TABLE}_01 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2023-01-02 00:00:00');
    CREATE TABLE {TARGET_TABLE}_02 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-02 00:00:00') TO ('2023-01-03 00:00:00');
    CREATE TABLE {TARGET_TABLE}_03 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-03 00:00:00') TO ('2023-01-04 00:00:00');
    CREATE TABLE {TARGET_TABLE}_04 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-04 00:00:00') TO ('2023-01-05 00:00:00');
    CREATE TABLE {TARGET_TABLE}_05 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-05 00:00:00') TO ('2023-01-06 00:00:00');
    CREATE TABLE {TARGET_TABLE}_06 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-06 00:00:00') TO ('2023-01-07 00:00:00');
    CREATE TABLE {TARGET_TABLE}_07 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-07 00:00:00') TO ('2023-01-08 00:00:00');
    CREATE TABLE {TARGET_TABLE}_08 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-08 00:00:00') TO ('2023-01-09 00:00:00');
    CREATE TABLE {TARGET_TABLE}_09 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-09 00:00:00') TO ('2023-01-10 00:00:00');
    CREATE TABLE {TARGET_TABLE}_10 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-10 00:00:00') TO ('2023-01-11 00:00:00');
    CREATE TABLE {TARGET_TABLE}_11 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-11 00:00:00') TO ('2023-01-12 00:00:00');
    CREATE TABLE {TARGET_TABLE}_12 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-12 00:00:00') TO ('2023-01-13 00:00:00');
    CREATE TABLE {TARGET_TABLE}_13 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-13 00:00:00') TO ('2023-01-14 00:00:00');
    CREATE TABLE {TARGET_TABLE}_14 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-14 00:00:00') TO ('2023-01-15 00:00:00');
    CREATE TABLE {TARGET_TABLE}_15 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-15 00:00:00') TO ('2023-01-16 00:00:00');
    CREATE TABLE {TARGET_TABLE}_16 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-16 00:00:00') TO ('2023-01-17 00:00:00');
    CREATE TABLE {TARGET_TABLE}_17 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-17 00:00:00') TO ('2023-01-18 00:00:00');
    CREATE TABLE {TARGET_TABLE}_18 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-18 00:00:00') TO ('2023-01-19 00:00:00');
    CREATE TABLE {TARGET_TABLE}_19 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-19 00:00:00') TO ('2023-01-20 00:00:00');
    CREATE TABLE {TARGET_TABLE}_20 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-20 00:00:00') TO ('2023-01-21 00:00:00');
    CREATE TABLE {TARGET_TABLE}_21 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-21 00:00:00') TO ('2023-01-22 00:00:00');
    CREATE TABLE {TARGET_TABLE}_22 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-22 00:00:00') TO ('2023-01-23 00:00:00');
    CREATE TABLE {TARGET_TABLE}_23 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-23 00:00:00') TO ('2023-01-24 00:00:00');
    CREATE TABLE {TARGET_TABLE}_24 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-24 00:00:00') TO ('2023-01-25 00:00:00');
    CREATE TABLE {TARGET_TABLE}_25 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-25 00:00:00') TO ('2023-01-26 00:00:00');
    CREATE TABLE {TARGET_TABLE}_26 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-26 00:00:00') TO ('2023-01-27 00:00:00');
    CREATE TABLE {TARGET_TABLE}_27 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-27 00:00:00') TO ('2023-01-28 00:00:00');
    CREATE TABLE {TARGET_TABLE}_28 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-28 00:00:00') TO ('2023-01-29 00:00:00');
    CREATE TABLE {TARGET_TABLE}_29 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-29 00:00:00') TO ('2023-01-30 00:00:00');
    CREATE TABLE {TARGET_TABLE}_30 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-30 00:00:00') TO ('2023-01-31 00:00:00');
    CREATE TABLE {TARGET_TABLE}_31 PARTITION OF {TARGET_TABLE} FOR VALUES FROM ('2023-01-31 00:00:00') TO ('2023-02-01 00:00:00');
"""

create_table = PostgresOperator(
    task_id="create_table",
    sql=create_table_query,
    dag=dag
)

etl_process = PythonOperator(
    task_id="etl_pipeline_ingestion",
    python_callable=etl_pipeline,
    dag=dag,
    execution_timeout=timedelta(seconds=3600),
)

# explore_data_task = PythonOperator(
#     task_id="explore_data",
#     python_callable=explore_data,
#     dag=dag
# )

# In case require drop table
# drop_table >> create_table >> etl_process >> explore_data_task
drop_table >> create_table >> etl_process

# In case unrequire drop table
# create_table >> etl_process >> explore_data_task
