import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
import concurrent.futures

# Create decorator for compute runtime
def runtime(func):
  def wrapper(*args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    runtime = end_time - start_time
    print(f"{func.__name__} took {runtime} seconds to execute.")
    return result
  return wrapper

class ETL:

  def __init__(self, source_file_path : str, connection : str, tgt_table : str, BatchSize : int = 5e3, last_date_for_loop : int = 9) -> None:

    """
    Initialize the ETL object.

    Parameters:
        - source_file_path (str): The path to the source file location.
        - connection (str): The Airflow connection ID for the PostgreSQL database.
        - tgt_table (str): The name of the target table for data ingestion.
        - BatchSize (int, optional): The size of batches for data insertion (default=1e4).
        - last_date_for_loop (int, optional): The last date of source file that require to ingest (defaut=9).
    """

    self.df = pd.DataFrame()
    self.source_file_path = source_file_path
    self.output_csv_path = f'{self.source_file_path}/output_csv' # Collect output csv file
    self.connection = connection
    self.tgt_table = tgt_table
    self.BatchSize = int(BatchSize)
    self.column_names = [] # Collect list of table's column name
    self.list_total_rec = [] # Collect list of total record in each file
    self.start_date = datetime(2023, 1, 1) # First date
    self.last_date_for_loop = last_date_for_loop # Last date

  # Create a function for connecting to the PostgreSQL database.
  def create_connection_postgresql(self) -> None:
    try:
      pg_hook = PostgresHook(postgres_conn_id=self.connection)
      conn = pg_hook.get_conn()
      return conn
    
    except Exception as e:
      print("Error connecting to PostgreSQL:", e)
      return None
    
  @runtime
  # Create a function for concat table after read parquet file
  def read_and_concat_batch(self, batch_files : list):
    """
    Read parquet file and concat into single table.

    Parameters:
        - batch_files (list): List of batch parquet file for read in each time that this function called.
    """
          
    batch_tables = [pq.read_table(os.path.join(self.source_file_path, file)) for file in batch_files]
    return pa.concat_tables(batch_tables)

  @runtime
  # Create a function for extract data.
  def extract_data(self, num_processes : int = 20):
      """
      Extract file from source file.

      Parameters:
          - num_processes (int): Maximum concurrent process.
      """

      print("########################")
      print("##### EXTRACT DATA #####")
      print("########################")

      print("Start extracting data ...")
      print("Listing parquet files in source file path ...")
      parquet_files = [file for file in os.listdir(self.source_file_path) if file.endswith('.parquet')]
      num_files = len(parquet_files)

      merged_table = None

      with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as executor:

        for i in range(0, num_files, self.BatchSize ):

          print(f"READ FILE STATUS ------ [{i}/{num_files}] ------")
          
          if i < num_files:
            batch_file_slices = parquet_files[i:i + self.BatchSize ]
          else:
            i = i - self.BatchSize
            batch_file_slices = parquet_files[i:num_files+1]

          futures = [executor.submit(self.read_and_concat_batch, batch_file_slices)]

          for future in concurrent.futures.as_completed(futures):

              batch_table = future.result()
              if merged_table is None:
                  merged_table = batch_table
              else:
                  merged_table = pa.concat_tables([merged_table, batch_table])

      print("Converting to Pandas DataFrame ...")
      self.df = merged_table.to_pandas()
      print('Data extraction complete')

  @runtime
  # Create a function for transform data.
  def transform_data(self) -> None:

    print("########################")
    print("#### TRANSFORM DATA ####")
    print("########################")

    # Convert datatype to ensure that these columns are in timestamp format
    print("Converting create_at to Timestamp ...")
    self.df['create_at'] = pd.to_datetime(self.df['create_at'], format='%Y-%m-%d %H:%M:%S')

    print("Converting product_expire to Timestamp ...")
    self.df['product_expire'] = pd.to_datetime(self.df['product_expire'], format='%Y-%m-%d %H:%M:%S')

    print("=======================================")
    print(f"Create csv file from filtered data ...")
    print("=======================================")

    # Check if the directory exists
    if not os.path.exists(self.output_csv_path):
        
        # If it doesn't exist, create the directory
        os.makedirs(self.output_csv_path)
        print(f"Directory '{self.output_csv_path}' created.")

    else:
        print(f"Directory '{self.output_csv_path}' already exists.")

    end_date = int(self.last_date_for_loop)

    for day in range(0, end_date): # Specfic date with in this range
        
        date = self.start_date + timedelta(days=day)
        
        # Format the current date as a string in 'YYYY-MM-DD' format
        formatted_date = date.strftime('%Y-%m-%d')
        file_csv = f"{self.output_csv_path}/data_date_{formatted_date}.csv"
        
        # Filter the DataFrame for the current date and assign it to the variable
        filtered_data = self.df[self.df['create_at'].dt.date == date.date()]

        # Collect list of total record in order to reconcile data 
        self.list_total_rec.append(filtered_data.shape[0])

        # Write output filtered_data into csv file
        filtered_data.to_csv(file_csv, index=False) 

  @runtime
  # Create a function for reconcile data.
  def reconcile_data(self, tgt_table : str, TOTAL_REC : int) -> None:
    """
    Reconcile data after loaded.

    Parameters:
        - tgt_table (str): Target table name.
        - TOTAL_REC (int): Source file total record.
    """

    print("########################")
    print("#### RECONCILE DATA ####")
    print("########################")

    print("++++++++++++++++++++++++")
    print(f"Target table : {tgt_table}")
    print("++++++++++++++++++++++++")

    conn = self.create_connection_postgresql()
    cur = conn.cursor()

    # Check record count in table
    cur.execute(f"SELECT COUNT(1) FROM {tgt_table}")
    result_rec_count = cur.fetchall()

    if result_rec_count[0][0] == TOTAL_REC:
      rec_count_status = "SUCCESS"
    else:
      rec_count_status = "FAILED"

    # Check null in table
    columns_str_chk_null = " IS NULL OR ".join(self.column_names) # Concatenate column name with IS NULL OR for create a condition check null
    CONDITION = columns_str_chk_null+" IS NULL" # for last column

    QUERY_CHK_NULL = f"""
        SELECT COUNT(1)
        FROM {tgt_table}
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
  
    cur.close()
    conn.close()

  @runtime
  # Create a function for data ingestion.
  def load_data(self) -> None:

    for column_name in self.df.columns:
      self.column_names.append(f'"{column_name}"')

    columns_str = ", ".join(self.column_names) # List all column separated by ", "
    
    print("====================================")
    print('List columns for insert into target:')
    print("++++++++++++++++++++++++++++++++++")
    print(f'{columns_str}')
    print("====================================")
    
    try:

      pg_hook_load = PostgresHook(postgres_conn_id=self.connection)

      end_date = int(self.last_date_for_loop)

      for day in range(0, end_date): # Specfic date with in this range

        date = self.start_date + timedelta(days=day)
        
        # Format the current date as a string in 'YYYY-MM-DD' format
        formatted_date_str = date.strftime('%Y-%m-%d')

        # Set the COPY command
        copy_sql = f"""
            COPY {self.tgt_table}_{day+1:02}({columns_str})
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """

        print(copy_sql)

        # Get the file path as a string
        file_path = f"{self.output_csv_path}/data_date_{formatted_date_str}.csv"

        # Open and execute the COPY command using the pg_hook
        print(f"======== START INGEST DATA INTO CHILD TABLE `{self.tgt_table}_{day+1:02}` ========")

        with open(file_path, 'r') as file:
            pg_hook_load.copy_expert(sql=copy_sql, filename=file.name)

        # self.reconcile_data(f'{self.tgt_table}_{day+1:02}', self.list_total_rec[day])

        print(f"======== FINISH INGEST DATA INTO CHILD TABLE `{self.tgt_table}_{day+1:02}` ========")

      # self.reconcile_data(f'{self.tgt_table}', sum(self.list_total_rec))

    except Exception as e:
        raise("Something went wrong", e)
    
      
