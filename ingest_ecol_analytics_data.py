# todo: fix primary key column not imported properly
# todo: ensure primary key is unique
# todo: some tables aren't populating (likely due to PK issues)

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
        
        logging.debug(f"Connecting to Analytics DB with connection string: {self.conn_str}")
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
        logging.debug("Closing eCollision Analytics DB connection.")
        self.conn.close()

    def get_table_columns(self, table_name):
        query = f"""
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name.lower()}'
        """
        logging.debug(f"Executing query to get columns for table: {table_name}. Query: {query}")
        headers, columns = self.query_without_param(query)
        logging.debug(f"Columns retrieved for {table_name}: {columns}")        
        return columns

    def get_constraints(self, table_name):
        query = f"""
        SELECT constraint_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = '{table_name.lower()}'
        """
        logging.debug(f"Getting constraints for table: {table_name}. Query: {query}")
        constraints = self.query_without_param(query)[1]
        logging.debug(f"Constraints retrieved for {table_name}: {constraints}")
        return constraints

class PostgreSQLDB:
    def __init__(self, user, password, host, database):
        logging.debug(f"Connecting to PostgreSQL DB at {host} with database: {database}")
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
            logging.debug("Transaction rolled back due to error.")
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
    # Prefix the table name with 'analytics_'
    prefixed_table_name = f"analytics_{table_name}"
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
        IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{prefixed_table_name.lower()}') THEN
            CREATE TABLE {prefixed_table_name} ({', '.join(all_defs)});
        END IF;
    END $$;
    """
    
    logging.debug(f"Generated CREATE TABLE query for {prefixed_table_name}: {create_query}")
    return create_query

def backup_analytics_to_postgres(tables=None, sample_size=None):
    try:
        logging.info("Starting backup operation from eCollision AnalyticsDB to PostgreSQL.")
        
        # Environment variable extraction
        analytics_db_driver = os.getenv('ECOLLISION_ANALYTICS_SQL_DRIVER')
        analytics_db_server = os.getenv('ECOLLISION_ANALYTICS_SQL_SERVER').replace('\\\\', '\\')
        analytics_db_name = os.getenv('ECOLLISION_ANALYTICS_SQL_DATABASE_NAME')
        analytics_db_trusted_connection = os.getenv('ECOLLISION_ANALYTICS_SQL_TRUSTED_CONNECTION')
        
        # Initialize eCollision Analytics
        logging.debug("Initializing AnalyticsDB connection.")
        analytics_db = AnalyticsDB(analytics_db_name, analytics_db_server, analytics_db_driver, analytics_db_trusted_connection)
        
        # Connect to PostgreSQL
        postgres_host = os.getenv('ECOLLISION_FUSION_SQL_HOST_NAME')
        postgres_db_name = os.getenv('ECOLLISION_FUSION_SQL_DATABASE_NAME')
        postgres_user = os.getenv('ECOLLISION_FUSION_SQL_USERNAME')
        postgres_password = os.getenv('ECOLLISION_FUSION_SQL_PASSWORD')
        
        logging.debug("Connecting to PostgreSQL DB.")
        postgres_db = PostgreSQLDB(postgres_user, postgres_password, postgres_host, postgres_db_name)

        if tables is None:
            analytics_db_tables_query = """
            SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
            """
            headers, tables = analytics_db.query_without_param(analytics_db_tables_query)

        if not tables:
            logging.warning("No tables found in the eCollision Analytics DB.")
            return

        for table in tables:
            table_name = table if isinstance(table, str) else table[0]
            logging.debug(f"Processing table: {table_name}")
            columns = analytics_db.get_table_columns(table_name)
            constraints = analytics_db.get_constraints(table_name)
            create_query = create_table_query(table_name, columns, constraints)

            try:
                logging.debug(f"Executing create table query for {table_name}.")
                postgres_db.execute_query(create_query)
            except Exception as e:
                logging.error(f"Failed to create table {table_name}: {e}")
                continue

            select_query = f"SELECT TOP {sample_size} * FROM [eCollisionAnalytics].[ECRDBA].{table_name}" if sample_size else f"SELECT * FROM {table_name}"
            logging.debug(f"Selecting data from {table_name}. Query: {select_query}")
            header, data = analytics_db.query_without_param(select_query)

            for row in data:
                insert_query = f"INSERT INTO analytics_{table_name} ({', '.join(header)}) VALUES ({', '.join(['%s'] * len(row))})"
                logging.debug(f"Inserting data into {table_name}: {row}")
                try:
                    postgres_db.execute_query(insert_query, row)
                except Exception as e:
                    logging.error(f"Failed to insert row into {table_name}: {row}. Error: {e}")

        # Closing connections
        logging.debug("Closing database connections.")
        analytics_db.close_connection()
        postgres_db.close_connection()
        logging.info("Backup operation completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the backup process: {e}")

if __name__ == "__main__":
    tables_to_backup = ['COLLISIONS', 'CL_OBJECTS', 'CLOBJ_PARTY_INFO', 'CLOBJ_PROPERTY_INFO', 'ECR_COLL_PLOTTING_INFO',
                        'CODE_TYPE_VALUES', 'CODE_TYPES', 'CL_STATUS_HISTORY', 'ECR_SYNCHRONIZATION_ACTION_ETL',
                        'ECR_SYNCHRONIZATION_ACTION_LOG_ETL']  # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    
    backup_analytics_to_postgres(tables=tables_to_backup, sample_size=100)
