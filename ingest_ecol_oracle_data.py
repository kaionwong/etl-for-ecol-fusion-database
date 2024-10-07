import cx_Oracle
import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
import logging

# Set up logging configuration
logging.basicConfig(level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

class OracleDB:
    def __init__(self, host, port, service):
        load_dotenv()
        self.username = os.getenv('ECOLLISION_ORACLE_SQL_USERNAME')
        self.password = os.getenv('ECOLLISION_ORACLE_SQL_PASSWORD')

        # Oracle Instant Client setup
        oracle_instant_client_dir = 'C:\\Users\\kai.wong\\_local_dev\\oracle_instant_client\\instantclient-basic-windows.x64-23.4.0.24.05\\instantclient_23_4'
        cx_Oracle.init_oracle_client(lib_dir=oracle_instant_client_dir)

        self.conn_str = f"{self.username}/{self.password}@//{host}:{port}/{service}"
        logging.debug(f"Connecting to Oracle DB with connection string: {self.conn_str}")
        self.conn = cx_Oracle.connect(self.conn_str)

    def query_without_param(self, query):
        logging.debug(f"Executing query: {query}")
        cursor = self.conn.cursor()
        result = cursor.execute(query).fetchall()
        header = [i[0] for i in cursor.description]
        cursor.close()
        logging.debug(f"Query executed successfully, fetched {len(result)} rows.")
        return header, result
    
    def close_connection(self):
        logging.debug("Closing Oracle DB connection.")
        self.conn.close()

    def get_table_columns(self, table_name):
        query = f"""
        SELECT column_name, data_type, data_length, nullable
        FROM all_tab_columns
        WHERE table_name = '{table_name.upper()}'
        AND owner = 'ECRDBA'
        ORDER BY column_id
        """
        logging.debug(f"Executing query to get columns: {query}")
        headers, columns = self.query_without_param(query)
        logging.debug(f"Columns retrieved for {table_name}: {columns}")
        return columns

    def get_constraints(self, table_name):
        query = f"""
        SELECT constraint_name, constraint_type, r_constraint_name 
        FROM user_constraints 
        WHERE table_name = '{table_name.upper()}'
        """
        logging.debug(f"Getting constraints for table: {table_name}")
        return self.query_without_param(query)[1]

    def get_table_owner(self, table_name):
        query = f"""
        SELECT owner 
        FROM all_tables 
        WHERE table_name = '{table_name.upper()}'
        """
        logging.debug(f"Getting owner for table: {table_name}")
        return self.query_without_param(query)[1][0][0]  # Returns the owner

class PostgreSQLDB:
    def __init__(self, host, database, user, password):
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

def map_oracle_to_postgres(data_type):
    """ Map Oracle data types to PostgreSQL data types """
    mapping = {
        'VARCHAR2': 'VARCHAR',
        'NUMBER': 'NUMERIC',
        'DATE': 'TIMESTAMP',
        'CHAR': 'CHAR',
        'FLOAT': 'DOUBLE PRECISION',
        # Add more mappings as needed
    }
    pg_type = mapping.get(data_type, 'TEXT')  # Default to TEXT if type not found
    logging.debug(f"Mapping Oracle type '{data_type}' to PostgreSQL type '{pg_type}'")
    return pg_type

def create_table_query(table_name, columns, constraints):
    """ Construct CREATE TABLE statement for PostgreSQL with a prefix."""
    prefixed_table_name = f"oracle_{table_name}"
    column_definitions = []
    primary_key_columns = []

    for column in columns:
        column_name, data_type, data_length, nullable = column
        pg_data_type = map_oracle_to_postgres(data_type)
        null_constraint = '' if nullable == 'Y' else 'NOT NULL'
        column_definitions.append(f"{column_name} {pg_data_type} {null_constraint}")

        if column_name.lower() == 'id':
            primary_key_columns.append(column_name)

    if primary_key_columns:
        primary_key_definition = f"PRIMARY KEY ({', '.join(primary_key_columns)})"
        constraints_definitions = [primary_key_definition]
    else:
        constraints_definitions = []

    for constraint in constraints:
        constraint_name, constraint_type, r_constraint_name = constraint
        if constraint_type == 'R':
            constraints_definitions.append(f"FOREIGN KEY ({constraint_name}) REFERENCES {r_constraint_name}")

    all_definitions = ",\n".join(column_definitions + constraints_definitions)
    create_query = f"CREATE TABLE IF NOT EXISTS {prefixed_table_name} (\n{all_definitions}\n);"
    logging.debug(f"Create table query for {prefixed_table_name}: {create_query}")
    return create_query

def backup_oracle_to_postgres(tables=None, sample_size=None):
    try:
        logging.info("Starting backup operation from Oracle to PostgreSQL.")

        # Initialize OracleDB with connection details
        oracle_db = OracleDB(
            host='ncc-production-qf14q-scan.exacc.ds.gov.ab.ca',
            port=1521,
            service='AT12PRD.WORLD'
        )
        
        # Connect to PostgreSQL (replace with your PostgreSQL connection details)
        postgres_host = 'localhost'
        postgres_db_name = 'ecollision_fusion_dev'
        postgres_user = os.getenv('ECOLLISION_FUSION_SQL_USERNAME')
        postgres_password = os.getenv('ECOLLISION_FUSION_SQL_PASSWORD')
        
        postgres_db = PostgreSQLDB(postgres_host, postgres_db_name, postgres_user, postgres_password)

        # If tables is None, get all tables from Oracle
        if tables is None:
            oracle_tables_query = """
            SELECT table_name FROM all_tables WHERE owner = 'ECRDBA'
            """
            # Get all tables from Oracle
            headers, tables = oracle_db.query_without_param(oracle_tables_query)

        for table in tables:
            table_name = table if isinstance(table, str) else table[0]

            # Get the owner of the table
            owner = oracle_db.get_table_owner(table_name)

            # Get column metadata from Oracle
            columns = oracle_db.get_table_columns(table_name)
            
            # Get constraints from Oracle
            constraints = oracle_db.get_constraints(table_name)

            # Construct PostgreSQL table creation query
            create_query = create_table_query(table_name, columns, constraints)
            
            # Create table in PostgreSQL
            logging.info(f"Creating table {table_name} in PostgreSQL.")
            postgres_db.execute_query(create_query)

            # Get data from Oracle table
            if sample_size is not None:
                data_query = f"SELECT * FROM {owner}.{table_name} WHERE ROWNUM <= {sample_size}"  # Sample data
            else:
                data_query = f"SELECT * FROM {owner}.{table_name}"  # Full data

            logging.debug(f"Fetching data from Oracle table: {table_name}")
            _, data = oracle_db.query_without_param(data_query)

            # Insert data into PostgreSQL table
            prefixed_table_name = f"oracle_{table_name}"
            insert_query = f"INSERT INTO {prefixed_table_name} ({', '.join([col[0] for col in columns])}) VALUES ({', '.join(['%s'] * len(columns))})"
            
            for row in data:
                try:
                    postgres_db.execute_query(insert_query, row)
                except Exception as e:
                    logging.error(f"Error inserting row into {prefixed_table_name}: {row}. Error: {e}")

        # Close connections
        oracle_db.close_connection()
        postgres_db.close_connection()
        logging.info("Backup operation completed successfully.")
    
    except Exception as e:
        logging.error(f"Backup operation failed: {e}")

if __name__ == "__main__":
    # Specify the tables to backup, or set to None to backup all tables
    # tables_to_backup = None  # Change this to a list of table names to specify, e.g., ['COLLISIONS', 'CL_OBJECTS'] 
    
    tables_to_backup = ['COLLISIONS', 'CL_OBJECTS', 'CLOBJ_PARTY_INFO', 'CLOBJ_PROPERTY_INFO', 'ECR_COLL_PLOTTING_INFO',
                        'CODE_TYPE_VALUES', 'CODE_TYPES', 'CL_STATUS_HISTORY', 'ECR_SYNCHRONIZATION_ACTION',
                        'ECR_SYNCHRONIZATION_ACTION_LOG']  # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    
    # tables_to_backup = ['COLLISIONS']  # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    
    backup_oracle_to_postgres(tables=tables_to_backup, sample_size=1280)  # Specify sample size or None for full data
