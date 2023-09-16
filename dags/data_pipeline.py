from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from etl_function import ETL

SOURCE_FILE_PATH = './data_sample/data_test'
TARGET_TABLE = 'test_tbl'
COLUMNS = ['department_name', 'sensor_serial', 'create_at', 'product_name', 'product_expire']
POSTGRES_CONN_ID = 'postgres_default'


etl_instance = ETL(source_file_path=SOURCE_FILE_PATH, connection=POSTGRES_CONN_ID, tgt_table=TARGET_TABLE)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry': 1,
}

dag = DAG(
    'ETL_process',
    default_args=default_args,
    description='ETL pipeline',
    schedule_interval=None
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
    sql=f"DROP TABLE IF EXISTS {TARGET_TABLE} ",
    dag=dag
)

create_table = PostgresOperator(
    task_id="create_table",
    sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        department_name VARCHAR(32), -- Data type length refer to function gen_text() in sampledata_new.py line 33
        sensor_serial VARCHAR(64), -- Data type length refer to function gen_text() in sampledata_new.py line 34
        create_at TIMESTAMP,
        product_name VARCHAR(16), -- Data type length refer to function gen_text() in sampledata_new.py line 35
        product_expire TIMESTAMP);
    """,
    dag=dag
)

etl_process = PythonOperator(
    task_id="etl_pipeline_ingestion",
    python_callable=etl_pipeline,
    # execution_timeout=timedelta(minutes=30),
    dag=dag
)

explore_data_task = PythonOperator(
    task_id="explore_data",
    python_callable=explore_data,
    dag=dag
)

# In case require drop table
drop_table >> create_table >> etl_process >> explore_data_task

# In case unrequire drop table
# create_table >> etl_process >> explore_data_task
