import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

class ETL:

  def __init__(self, source_file_path : str, connection : str, tgt_table : str, BatchSize : int = 2e6) -> None:

    """
    Initialize the ETL object.

    Parameters:
        - source_file_path (str): The path to the source file location.
        - connection (str): The Airflow connection ID for the PostgreSQL database.
        - tgt_table (str): The name of the target table for data ingestion.
        - BatchSize (int, optional): The size of batches for data insertion (default=2e6).
    """

    self.df = pd.DataFrame()
    self.USER = os.getenv("POSTGRES_USER")
    self.PASSWORD = os.getenv("POSTGRES_PASSWORD")
    self.DB = os.getenv("POSTGRES_DB")
    self.source_file_path = source_file_path
    self.connection = connection
    self.tgt_table = tgt_table
    self.BatchSize = BatchSize

  # Create a function for connecting to the PostgreSQL database.

  def create_connection_postgresql(self):
    try:
      pg_hook = PostgresHook(postgres_conn_id=self.connection)
      conn = pg_hook.get_conn()
      return conn
    
    except Exception as e:
      print("Error connecting to PostgreSQL:", e)
      return None
  
  # Create a function to execute various queries, mainly to catch errors that may occur when running queries related to DELETE, UPDATE, INSERT to prevent data loss.

  def execute_query(self, conn, cur, QUERY : str, VALUES : tuple = None):

    """
    Execute query.

    Parameters:
        - conn: A PostgreSQL database connection.
        - cur: A database cursor.
        - QUERY (str): The SQL query statement to execute.
        - VALUES (tuple, optional): Optional tuple of values to insert into placeholders in the query (default=None).
    """

    try:
      cur.execute(QUERY, VALUES)
      conn.commit()
    except Exception as e:
      conn.rollback()
      print(f"Execute query Failed", e)

  # Create a function for extract data.
  def extract_data(self):

    print("########################")
    print("##### EXTRACT DATA #####")
    print("########################")
    print("Start extracting data ...")
    list_file = os.listdir(self.source_file_path)

    # List only parquet file in folder to aviod other file in case that folder contain various extension file
    parquet_files = [file for file in list_file if file.endswith('.parquet')]

    # Combine Parquet files into a single dataset
    start_time = datetime.now()

    # Apply PyArrow library for handling large datasets using Arrow memory structures. 
    tables = [pq.read_table(os.path.join(self.source_file_path, file)) for file in parquet_files]

    table = pa.concat_tables(tables)

    # Convert the combined PyArrow Table to a Pandas self.df
    self.df = table.to_pandas()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f'Complete extract data with duration: {duration}')
    print('Exlpore data ...')
    print(("========================"))
    print(f'Total row: {self.df.shape[0]}')
    print(f'Total column: {self.df.shape[1]}')
    print(("========================"))
    print(f'\n')
    print('Show information of Dataframe')
    print(("========================"))
    print(self.df.info())
    print(("========================"))
    print(f'\n')
    print('Show example data')
    print(("========================"))
    print(self.df.head(5))
    print(("========================"))
    print(f'\n')


  # Create a function for transform data.
  def transform_data(self):

    print("########################")
    print("#### TRANSFORM DATA ####")
    print("########################")
    print(("========================"))
    print(self.df.info())
    print(("========================"))
    # Convert datatype to ensure that these columns are in timestamp format
    self.df['create_at'] = self.df['create_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    self.df['product_expire'] = self.df['product_expire'].dt.strftime('%Y-%m-%d %H:%M:%S')


  # Create a function for data ingestion.
  def load_data(self, Truncate = True) -> None:

    """
    Ingestion data into target table.

    Parameters:
        - Truncate (boolean, optional): If True, truncate the target table before ingestion. If False, append data to the table (default=True).
    """

    conn = self.create_connection_postgresql()
    cur = conn.cursor()

    TOTAL_REC = len(self.df)


    # Truncate the data every time, under the assumption that there is only one set of data for ingestion.
    # If in the future there is a need to insert additional data, this section will need to be modified.
    if Truncate:
      try:
        print(f"START TRUNCATE TABLE `{self.tgt_table}` ....")
        cur.execute(f"TRUNCATE TABLE {self.tgt_table}")
        conn.commit()
        print(f"TRUNCATE TABLE `{self.tgt_table}` >> COMPLETE")
      except Exception as e:
        print(f"TRUNCATE TABLE `{self.tgt_table}` >> FAILED", e)

    print(f"======== START INGEST DATA INTO `{self.DB}`.`{self.tgt_table}` ========")

    try:
      for index, row in self.df.iterrows():
        column_names = []
        values = []

        for column_name in self.df.columns:

            # Enclose column names with "" for case-sensitivity
            column_names.append(f'"{column_name}"')
            values.append(row[column_name])

        columns_str = ", ".join(column_names)

        # The data contains various data types, and these placeholders will be used in a join operation to create a set of columns.
        placeholders = ", ".join(["%s"] * len(values))
        values = tuple(values)

        QUERY_INSERT = f"INSERT INTO {self.tgt_table} ({columns_str}) VALUES ({placeholders})"
        self.execute_query(conn, cur, QUERY_INSERT, values)  # Use parameterized query with values list

        if (index % self.BatchSize == 1+0e-2 ) or ((index + 1) % TOTAL_REC ==  0):
          conn.commit()

          message : str = f"ROWS INSERT STATUS ------ [{index}/{TOTAL_REC}] ------"
          print(message)


      # Check record count in table
      cur.execute(f"SELECT COUNT(1) FROM {self.tgt_table}")
      result_rec_count = cur.fetchall()

      if result_rec_count[0][0] == TOTAL_REC:
        rec_count_status = "SUCCESS"
      else:
        rec_count_status = "FAILED"

      # Check null in table
      # Concatenate column name with IS NULL OR for create a condition check null
      columns_str_chk_null = " IS NULL OR ".join(column_names)
      CONDITION = columns_str_chk_null+" IS NULL" # for last column

      QUERY_CHK_NULL = f"""
          SELECT COUNT(*)
          FROM {self.tgt_table}
          WHERE {CONDITION}
      """

      cur.execute(QUERY_CHK_NULL)
      result_chk_null = cur.fetchall()


      if result_chk_null[0][0] == 0:
        chk_null_status = "SUCCESS"
      else:
        chk_null_status = "FAILED"

      print(f"======== RECONCILE PROCESS ========")
      print(f"Source rows = {TOTAL_REC}")
      print(f"Target rows = {result_rec_count[0][0]}")
      print(f"Record count status = {rec_count_status}")
      print(f"Check null status = {chk_null_status}")

    except Exception as e:
        raise("Something went wrong", e)
    
    cur.close()
    conn.close()

