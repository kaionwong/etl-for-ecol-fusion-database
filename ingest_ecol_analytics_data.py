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
    def __init__(self, database_name, database_server, database_driver, database_trusted_connection):
        self.conn_str = ''
        self.conn_str += f'Driver={database_driver};'
        self.conn_str += f'Server={database_server};'
        self.conn_str += f'Database={database_name};'
        self.conn_str += f'Trusted_Connection={database_trusted_connection};'
        
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
        # Modify the query to retrieve table metadata for the new database
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
        # Modify to fetch constraints for the new database
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
    """ Map new source database types to PostgreSQL data types """
    mapping = {
        'varchar': 'VARCHAR',
        'int': 'INTEGER',
        'float': 'DOUBLE PRECISION',
        'timestamp': 'TIMESTAMP',
        # Add more mappings as needed for the new DB
    }
    pg_type = mapping.get(data_type.lower(), 'TEXT')  # Default to TEXT if type not found
    logging.debug(f"Mapping new DB type '{data_type}' to PostgreSQL type '{pg_type}'")
    return pg_type

def create_table_query(table_name, columns, constraints):
    """ Construct CREATE TABLE statement for PostgreSQL """
    # Same logic as before

def backup_analytics_to_postgres(tables=None, sample_size=None):
    try:
        logging.info("Starting backup operation from New Source DB to PostgreSQL.")

        analytics_db_driver = os.getenv('ECOLLISION_ANALYTICS_SQL_DRIVER')
        analytics_db_server = os.getenv('ECOLLISION_ANALYTICS_SQL_SERVER').replace('\\\\', '\\')
        analytics_db_name = os.getenv('ECOLLISION_ANALYTICS_SQL_DATABASE_NAME')
        analytics_db_trusted_connection = os.getenv('ECOLLISION_ANALYTICS_SQL_TRUSTED_CONNECTION')

        # Initialize NewSourceDB with connection details
        analytics_db = AnalyticsDB(analytics_db_name, analytics_db_server, analytics_db_driver, analytics_db_trusted_connection)

        # Connect to PostgreSQL (replace with your PostgreSQL connection details)
        postgres_host = os.getenv('ECOLLISION_FUSION_SQL_HOST_NAME')
        postgres_db_name = os.getenv('ECOLLISION_FUSION_SQL_DATABASE_NAME')
        postgres_user = os.getenv('ECOLLISION_FUSION_SQL_USERNAME')
        postgres_password = os.getenv('ECOLLISION_FUSION_SQL_PASSWORD')
        
        postgres_db = PostgreSQLDB(postgres_user, postgres_password, postgres_host, postgres_db_name)

        if tables is None:
            # Query to get all tables for the new database
            new_db_tables_query = """
            SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
            """
            headers, tables = analytics_db.query_without_param(new_db_tables_query)

        for table in tables:
            table_name = table if isinstance(table, str) else table[0]

            # Get column metadata and constraints
            columns = analytics_db.get_table_columns(table_name)
            constraints = analytics_db.get_constraints(table_name)

            # Construct PostgreSQL table creation query
            create_query = create_table_query(table_name, columns, constraints)
            postgres_db.execute_query(create_query)

            # Get data from New DB table
            if sample_size is not None:
                data_query = f"SELECT * FROM {table_name} LIMIT {sample_size}"  # Sample data
            else:
                data_query = f"SELECT * FROM {table_name}"  # Full data

            logging.debug(f"Fetching data from New Source DB table: {table_name}")
            _, data = analytics_db.query_without_param(data_query)

            # Insert data into PostgreSQL table
            prefixed_table_name = f"analytics_{table_name}"
            insert_query = f"INSERT INTO {prefixed_table_name} ({', '.join([col[0] for col in columns])}) VALUES ({', '.join(['%s'] * len(columns))})"
            
            for row in data:
                try:
                    postgres_db.execute_query(insert_query, row)
                except Exception as e:
                    logging.error(f"Error inserting row into {prefixed_table_name}: {row}. Error: {e}")

        # Close connections
        analytics_db.close_connection()
        postgres_db.close_connection()
        logging.info("Backup operation completed successfully.")
    
    except Exception as e:
        logging.error(f"Backup operation failed: {e}")

if __name__ == "__main__":
    # Specify the tables to backup, or set to None to backup all tables
    # tables_to_backup = None  # Change this to a list of table names to specify, e.g., ['COLLISIONS', 'CL_OBJECTS'] 
    
    # tables_to_backup = ['COLLISIONS', 'CL_OBJECTS', 'CLOBJ_PARTY_INFO', 'CLOBJ_PROPERTY_INFO', 'ECR_COLL_PLOTTING_INFO',
    #                     'CODE_TYPE_VALUES', 'CODE_TYPES', 'CL_STATUS_HISTORY', 'ECR_SYNCHRONIZATION_ACTION_ETL',
    #                     'ECR_SYNCHRONIZATION_ACTION_LOG_ETL']  # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    
    tables_to_backup = ['COLLISIONS']  # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    
    backup_analytics_to_postgres(tables=tables_to_backup, sample_size=125)  # Specify sample size or None for full data