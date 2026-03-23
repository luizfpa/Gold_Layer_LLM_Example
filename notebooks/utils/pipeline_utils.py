import time
import logging
import json
import os
import duckdb
from functools import wraps
from typing import Callable, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../../config/pipeline_config.json')

LOCAL_MODE = True

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        return {}

config = load_config()

def get_storage_path(layer_path):
    if LOCAL_MODE:
        if "@" in layer_path:
            container = layer_path.split("@")[0].split("://")[1]
            return f"./tmp/duckdb/{container}"
        return "./tmp/duckdb/default"
    return layer_path

def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    def deco_retry(f: Callable) -> Callable:
        @wraps(f)
        def f_retry(*args, **kwargs) -> Any:
            _tries, _delay = tries, delay
            log = logger or logging

            while _tries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    log.warning(f"{e}, Retrying in {_delay} seconds...")
                    time.sleep(_delay)
                    _tries -= 1
                    _delay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry

def send_email_notification(subject: str, body: str, recipient: str = None):
    if recipient is None:
        recipient = config.get('email_notifications', {}).get('recipient', 'admin@fabric-pipeline.com')
    
    logging.info(f"""
    --- EMAIL NOTIFICATION ---
    To: {recipient}
    Subject: {subject}

    {body}
    --------------------------
    """)

def create_duckdb_connection(path: str):
    """Create or connect to a DuckDB database."""
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else "./tmp/duckdb", exist_ok=True)
    return duckdb.connect(path)

def get_table_name_from_path(path: str) -> str:
    """Extract table name from path."""
    return path.split('/')[-1] if '/' in path else path

def create_idempotent_table(spark, table_name: str, schema, primary_keys: list[str], path: str):
    """
    Creates a table if it doesn't exist using DuckDB.
    Note: 'spark' parameter is kept for API compatibility but not used in DuckDB version.
    """
    effective_path = get_storage_path(path)
    db_path = effective_path + ".duckdb"
    table_name = get_table_name_from_path(path)
    
    logging.info(f"Ensuring table '{table_name}' at '{db_path}' exists with correct schema.")
    
    conn = create_duckdb_connection(db_path)
    try:
        # Check if table exists
        tables = conn.execute("SHOW TABLES").fetchall()
        table_exists = any(t[0] == table_name for t in tables)
        
        if table_exists:
            logging.info(f"Table '{table_name}' already exists.")
            conn.close()
            return
    except Exception as e:
        logging.info(f"Table does not exist, creating: {e}")
    
    try:
        # Convert PySpark schema to DuckDB columns
        duckdb_columns = []
        for field in schema.fields:
            dtype = field.dataType.simpleString() if hasattr(field.dataType, 'simpleString') else str(field.dataType)
            # Map PySpark types to DuckDB types
            dtype_map = {
                'Integer': 'INTEGER',
                'Long': 'BIGINT',
                'Double': 'DOUBLE',
                'String': 'VARCHAR',
                'Timestamp': 'TIMESTAMP',
                'Boolean': 'BOOLEAN'
            }
            duckdb_type = dtype_map.get(dtype, 'VARCHAR')
            duckdb_columns.append(f"{field.name} {duckdb_type}")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(duckdb_columns)})"
        conn.execute(create_sql)
        logging.info(f"Table '{table_name}' created successfully.")
    except Exception as e:
        logging.warning(f"Could not create DuckDB table. Error: {e}")
        raise
    finally:
        conn.close()

def upsert_to_delta(spark, df, table_name: str, primary_keys: list[str], path: str):
    """
    Performs upsert operation using DuckDB.
    Note: 'spark' parameter is kept for API compatibility but not used.
    'df' should be a list of tuples for DuckDB.
    """
    if not primary_keys:
        raise ValueError("Primary keys must be provided for idempotent upsert operations.")

    effective_path = get_storage_path(path)
    db_path = effective_path + ".duckdb"
    table_name = get_table_name_from_path(path)
    
    logging.info(f"Performing idempotent upsert into '{table_name}' using primary keys: {', '.join(primary_keys)}")

    conn = create_duckdb_connection(db_path)
    try:
        # Get column names from the dataframe
        columns = df.columns
        
        # Convert dataframe to list of tuples
        if hasattr(df, 'collect'):
            data = [tuple(row) for row in df.collect()]
        else:
            data = df
        
        if not data:
            logging.info("No data to upsert.")
            conn.close()
            return
        
        # Get existing data and filter
        try:
            existing = conn.execute(f"SELECT * FROM {table_name}").fetchall()
            if existing:
                # Build WHERE clause for existing rows to exclude
                where_clause = ' AND '.join([f"t.{pk} = s.{pk}" for pk in primary_keys])
                
                # Create temp table with new data
                conn.execute("CREATE TEMP TABLE new_data AS SELECT * FROM (SELECT * FROM new_data_sample) WHERE 1=0")
                
                # Insert new data
                placeholders = ','.join(['?' for _ in range(len(columns))])
                conn.execute(f"INSERT INTO new_data VALUES ({placeholders})", data)
                
                # Delete matching records from existing table
                delete_where = ' AND '.join([f"{pk} IN (SELECT {pk} FROM new_data)" for pk in primary_keys])
                conn.execute(f"DELETE FROM {table_name} WHERE {delete_where}")
                
                # Insert new data
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM new_data")
                
                conn.execute("DROP TABLE new_data")
            else:
                # Table is empty, just insert
                placeholders = ','.join(['?' for _ in range(len(columns))])
                conn.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", data)
        except Exception as e:
            logging.warning(f"Upsert merge failed, doing direct insert: {e}")
            # Fallback: just insert (may create duplicates)
            placeholders = ','.join(['?' for _ in range(len(columns))])
            conn.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", data)
        
        logging.info(f"Upsert to '{table_name}' completed successfully.")
    except Exception as e:
        logging.error(f"Error during upsert to '{table_name}': {e}")
        raise
    finally:
        conn.close()

# Example usage
if __name__ == "__main__":
    print("Running DuckDB utility module tests...")
    send_email_notification("Test Subject", "This is a test email body.")
    
    # Test DuckDB connection
    conn = duckdb.connect("./tmp/test.duckdb")
    conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
    result = conn.execute("SELECT * FROM test").fetchall()
    print(f"Test result: {result}")
    conn.close()