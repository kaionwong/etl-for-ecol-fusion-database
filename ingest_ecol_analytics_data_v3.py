import psycopg2
import pyodbc
import pandas as pd
from dotenv import load_dotenv
import os
import logging

# Set up logging configuration
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

class AnalyticsDB:
    def __init__(self, db_name, db_server, db_driver, db_trusted_connection):
        self.conn_str = ''
        self.conn_str += f'Driver={db_driver};'
        self.conn_str += f'Server={db_server};'
        self.conn_str += f'Database={db_name};'
        self.conn_str += f'Trusted_Connection={db_trusted_connection};'
    
        self.conn = pyodbc.connect(self.conn_str)

    def query_without_param(self, query):
        logging.debug(f"Executing query: {query}")
        cursor = self.conn.cursor()
        result = cursor.execute(query).fetchall()
        header = [i[0] for i in cursor.description]
        cursor.close()
        logging.debug(f"Query executed successfully, fetched {len(result)} rows.")
        return header, result
    
    def close_connection(self):
        logging.debug("Closing New Source DB connection.")
        self.conn.close()

    def get_table_columns(self, table_name):
        query = f"""
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name.lower()}'
        """
        logging.debug(f"Executing query to get columns: {query}")
        headers, columns = self.query_without_param(query)
        logging.debug(f"Columns retrieved for {table_name}: {columns}")        
        return columns

    def get_constraints(self, table_name):
        query = f"""
        SELECT constraint_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = '{table_name.lower()}'
        """
        logging.debug(f"Getting constraints for table: {table_name}")
        return self.query_without_param(query)[1]

class PostgreSQLDB:
    def __init__(self, user, password, host, database):
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        self.conn.autocommit = False  # Disable autocommit, we will handle transactions manually
        logging.debug("Connected to PostgreSQL DB.")

    def execute_query(self, query, data=None):
        cursor = self.conn.cursor()
        try:
            logging.debug(f"Executing query: {query} with data: {data}")
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
        except Exception as e:
            logging.error(f"Error executing query: {query}. Error: {e}")
            self.conn.rollback()  # Rollback the transaction on failure
            raise  # Re-raise the exception after rollback
        else:
            self.conn.commit()  # Commit the transaction if no errors occur
            logging.debug("Query executed and committed successfully.")
        finally:
            cursor.close()

    def close_connection(self):
        logging.debug("Closing PostgreSQL DB connection.")
        self.conn.close()

def map_analytics_db_to_postgres(data_type):
    """ Map MS SQL Server types to PostgreSQL data types """
    mapping = {
        # String types
        'varchar': 'VARCHAR',
        'nvarchar': 'VARCHAR',            # SQL Server nvarchar
        'char': 'CHAR',
        'nchar': 'CHAR',
        'text': 'TEXT',
        'ntext': 'TEXT',

        # Integer types
        'int': 'INTEGER',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',            # PostgreSQL does not have TINYINT; SMALLINT is closest
        'bigint': 'BIGINT',

        # Decimal and float types
        'decimal': 'DECIMAL',
        'numeric': 'NUMERIC',
        'float': 'DOUBLE PRECISION',
        'real': 'REAL',                   # SQL Server REAL -> PostgreSQL REAL
        
        # Date and time types
        'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP',         # SQL Server datetime2 -> PostgreSQL TIMESTAMP
        'smalldatetime': 'TIMESTAMP',     # SQL Server smalldatetime -> PostgreSQL TIMESTAMP
        'date': 'DATE',
        'time': 'TIME',

        # Boolean types
        'bit': 'BOOLEAN',                 # SQL Server bit -> PostgreSQL BOOLEAN

        # Binary types
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
        'image': 'BYTEA',

        # Other types
        'uniqueidentifier': 'UUID',       # SQL Server uniqueidentifier -> PostgreSQL UUID
        'xml': 'XML',
        'money': 'NUMERIC',               # SQL Server money -> PostgreSQL NUMERIC
        'smallmoney': 'NUMERIC',          # SQL Server smallmoney -> PostgreSQL NUMERIC
    }

    # Default to TEXT if the type is not mapped
    pg_type = mapping.get(data_type.lower(), 'TEXT')  
    logging.debug(f"Mapping MS SQL Server type '{data_type}' to PostgreSQL type '{pg_type}'")
    return pg_type

def create_table_query(table_name, columns, constraints):
    column_defs = []
    primary_keys = []
    unique_constraints = []

    for column in columns:
        col_name = column[0]
        data_type = map_analytics_db_to_postgres(column[1])
        nullable = 'NOT NULL' if column[3] == 'NO' else ''  # Only append NOT NULL if applicable
        column_defs.append(f"{col_name} {data_type} {nullable}".strip())  # Strip to avoid extra spaces
    
    for constraint in constraints:
        constraint_name = constraint[0]
        constraint_type = constraint[1]
        if constraint_type == 'PRIMARY KEY':
            primary_keys.append(constraint_name)  # Use actual constraint name for primary key
        elif constraint_type == 'UNIQUE':
            unique_constraints.append(constraint_name)  # Handle unique constraints as needed

    if primary_keys:
        column_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    if unique_constraints:
        for unique_col in unique_constraints:
            column_defs.append(f"UNIQUE ({unique_col})")  # Add unique constraints as necessary

    all_defs = column_defs
    create_query = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name.lower()}') THEN
            CREATE TABLE {table_name} ({', '.join(all_defs)});
        END IF;
    END $$;
    """
    
    logging.debug(f"Generated CREATE TABLE query: {create_query}")
    return create_query

def backup_analytics_to_postgres(tables=None, sample_size=None):
    try:
        logging.info("Starting backup operation from New Source DB to PostgreSQL.")
        
        # Environment variable extraction
        analytics_db_driver = os.getenv('ECOLLISION_ANALYTICS_SQL_DRIVER')
        analytics_db_server = os.getenv('ECOLLISION_ANALYTICS_SQL_SERVER').replace('\\\\', '\\')
        analytics_db_name = os.getenv('ECOLLISION_ANALYTICS_SQL_DATABASE_NAME')
        analytics_db_trusted_connection = os.getenv('ECOLLISION_ANALYTICS_SQL_TRUSTED_CONNECTION')
        
        # Initialize NewSourceDB
        analytics_db = AnalyticsDB(analytics_db_name, analytics_db_server, analytics_db_driver, analytics_db_trusted_connection)
        
        # Connect to PostgreSQL
        postgres_host = os.getenv('ECOLLISION_FUSION_SQL_HOST_NAME')
        postgres_db_name = os.getenv('ECOLLISION_FUSION_SQL_DATABASE_NAME')
        postgres_user = os.getenv('ECOLLISION_FUSION_SQL_USERNAME')
        postgres_password = os.getenv('ECOLLISION_FUSION_SQL_PASSWORD')
        
        postgres_db = PostgreSQLDB(postgres_user, postgres_password, postgres_host, postgres_db_name)

        if tables is None:
            new_db_tables_query = """
            SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
            """
            headers, tables = analytics_db.query_without_param(new_db_tables_query)

        if not tables:
            logging.warning("No tables found in the New Source DB.")
            return

        for table in tables:
            table_name = table if isinstance(table, str) else table[0]
            columns = analytics_db.get_table_columns(table_name)
            constraints = analytics_db.get_constraints(table_name)
            create_query = create_table_query(table_name, columns, constraints)

            try:
                postgres_db.execute_query(create_query)
            except Exception as e:
                if "relation" in str(e) and "already exists" in str(e):
                    logging.warning(f"Table {table_name} already exists, skipping creation.")
                else:
                    logging.error(f"Error creating table {table_name}: {e}")
                    continue
            
            # Construct data fetching query
            data_query = f"SELECT * FROM {table_name} LIMIT {sample_size}" if sample_size else f"SELECT * FROM {table_name}"
            logging.debug(f"Fetching data from New Source DB table: {table_name}")
            _, data = analytics_db.query_without_param(data_query)

            if not data:
                logging.warning(f"No data found in table: {table_name}")
                continue
            
            prefixed_table_name = f"analytics_{table_name}"
            insert_query = f"INSERT INTO {prefixed_table_name} ({', '.join([col[0] for col in columns])}) VALUES ({', '.join(['%s'] * len(columns))})"
            
            for row in data:
                try:
                    postgres_db.execute_query(insert_query, row)
                except Exception as e:
                    logging.error(f"Error inserting row into {prefixed_table_name}: {row}. Error: {e}")

    except Exception as e:
        logging.error(f"Backup operation failed: {e}")
    
    finally:
        # Ensure connections are closed in case of an error
        if 'analytics_db' in locals():
            analytics_db.close_connection()
        if 'postgres_db' in locals():
            postgres_db.close_connection()
        logging.info("Backup operation completed successfully.")

if __name__ == "__main__":
    # Specify the tables to backup, or set to None to backup all tables
    tables_to_backup = ['COLLISIONS']  # Change this to a list of table names to specify
    
    backup_analytics_to_postgres(tables=tables_to_backup, sample_size=2)  # Specify sample size or None for full data
