import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import logging
import time
from multiprocessing import Pool
# 1. Export from MSSQL to CSV


logging.basicConfig(filename='data_migration.log', level=logging.INFO)
# Database connection parameters for MSSQL
DATABASE = 'DB'
USERNAME = 'YourUsername'
PASSWORD = 'YourPassword'
SERVER = 'localhost'  # or your server name or IP address
DRIVER = 'ODBC Driver 17 for SQL Server' 

# Creating the connection string and engine for MSSQL
connection_string = f'mssql+pyodbc://{SERVER}/{DATABASE}?driver={DRIVER}'
export_engine = create_engine(connection_string)

# Initialize the inspector and get the table names
inspector = inspect(export_engine)
table_names = inspector.get_table_names()
TRACKING_FILE = "exported_tables.txt"

# Load already processed tables into a set for faster lookup
if os.path.exists(TRACKING_FILE):
    with open(TRACKING_FILE, "r") as f:
        processed_tables = set(f.read().splitlines())
else:
    processed_tables = set()
csv_output_filepaths = []  # To store generated CSV paths
EXCLUDED_TABLES = [] # handles excluded tables 

def get_sql_server_schema(engine, table_name):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    ddl_statements = []
    for col in columns:
        # Map SQL Server data types to Snowflake data types here
        # Note: This is a simplified mapper and might need more adjustments
        dtype_mapping = {
            'bigint': 'BIGINT',
            'bit': 'BOOLEAN',
            'decimal': 'FLOAT',
            'int': 'INTEGER',
            'money': 'FLOAT',
            'numeric': 'NUMBER',
            'smallint': 'SMALLINT',
            'smallmoney': 'FLOAT',
            'tinyint': 'TINYINT',
            'float': 'FLOAT',
            'real': 'FLOAT',
            'date': 'DATE',
            'datetime': 'TIMESTAMP_NTZ',
            'datetime2': 'TIMESTAMP_NTZ',
            'smalldatetime': 'TIMESTAMP_NTZ',
            'time': 'TIME',
            'char': 'TEXT',
            'varchar': 'TEXT',
            'text': 'TEXT',
            'nchar': 'CHAR',
            'nvarchar': 'VARCHAR',
            'ntext': 'TEXT',
            'binary': 'BINARY',
            'varbinary': 'VARBINARY',
            'image': 'BINARY',
            'uniqueidentifier': 'TEXT',
            'xml': 'TEXT',
            'sql_variant': 'VARIANT',
            'hierarchyid': 'TEXT'
        }
        data_type = str(col['type']).lower().split('.')[-1]
        snowflake_dtype = dtype_mapping.get(data_type, 'TEXT')
        ddl_statements.append(f"{col['name']} {snowflake_dtype}")

    ddl = f"CREATE TABLE {table_name} ({', '.join(ddl_statements)})"
    return ddl

def table_exists_in_snowflake(engine, table_name):
    connection = engine.connect()
    result = connection.execute(f"SHOW TABLES LIKE '{table_name}'")
    return result.rowcount > 0


# Iterate over each table and export to CSV
for table_name in table_names:
    if table_name not in EXCLUDED_TABLES and table_name not in processed_tables:
        csv_path = f"D:/Repos/Data_Loads/Data_Pipeline/Data_Pipeline/Data/{table_name}.csv"
        csv_output_filepaths.append(csv_path)  # Store path for later
    
        # Query the table content
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, export_engine)
    
        # Export the dataframe to CSV
        df.to_csv(csv_path, index=False)
        with open(TRACKING_FILE, "a") as f:
            f.write(f"{table_name}\n")
        print(f"Exported {table_name} to {csv_path}")
        logging.info(f"Exported {table_name} to {csv_path}")

# Close the MSSQL connection
export_engine.dispose()

# 2. Import from CSV to Snowflake
data_dir = "D:/Repos/Data_Loads/Data_Pipeline/Data_Pipeline/Data/"
csv_filepaths = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
# Setup an SQL Alchemy Engine object for Snowflake
import_engine = create_engine(
    'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
        u='Username',
        p='Password',
        a='Account',
        r='Role',
        d='DB',
        s='Schema',
        w='Warehouse',
    )
)

# Define the upload function
def upload_to_snowflake(data, table_name, engine):
    data.to_sql(table_name, engine, index=False, if_exists='append', chunksize=15000)


# Define maximum number of retries
max_retries = 1

def process_csv_path(path):
    filename, _ext = os.path.splitext(os.path.basename(path))
    data = pd.read_csv(path)
    retries = 0
    success = False
    while not success and retries < max_retries:
        try:
            upload_to_snowflake(data, filename, import_engine)
            success = True
            os.remove(path)  # Remove file only after successful upload
        except Exception as e:
            retries += 1
            if retries < max_retries:
                time.sleep(10)  # Wait for 10 seconds before retrying
            else:
                print(f"Failed to upload {filename} after {max_retries} attempts. Error: {e}")
        
        finally:
            # Tear down Snowflake connection gracefully pre-exit
            import_engine.dispose()


if __name__ == '__main__':
    pool = Pool(processes=10)  # change to the number of cores you want to use
    pool.map(process_csv_path, csv_filepaths)
    pool.close()
    pool.join()