import pandas as pd
from dotenv import load_dotenv
import os

import logging

from helper import time_execution
from helper_db_operation import OracleDB, PostgreSQLDB, map_oracle_to_postgres

# Set up logging configuration
logging.basicConfig(level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

def create_oracle_table_query(table_name, columns, constraints, dev_mode=False):
    """ Construct CREATE TABLE statement for PostgreSQL with a dev prefix if dev_mode is True."""
    # Apply prefix based on dev_mode
    prefixed_table_name = f"{'oracle_' + table_name}_dev" if dev_mode else f"oracle_{table_name}"
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

@time_execution
def backup_oracle_to_postgres(tables=None, sample_size=None, drop_existing=False, dev_mode=False):
    try:
        logging.info("Starting backup operation from Oracle to PostgreSQL.")
        
        # Database connection setup
        oracle_username = os.getenv('ECOLLISION_ORACLE_SQL_USERNAME')
        oracle_password = os.getenv('ECOLLISION_ORACLE_SQL_PASSWORD')
        oracle_host = os.getenv('ECOLLISION_ORACLE_SQL_HOST_NAME')
        oracle_port = os.getenv('ECOLLISION_ORACLE_SQL_PORT')
        oracle_service = os.getenv('ECOLLISION_ORACLE_SQL_SERVICE_NAME')

        oracle_db = OracleDB(
            oracle_username, oracle_password, oracle_host, oracle_port, oracle_service
        )
        
        postgres_host = os.getenv('ECOLLISION_FUSION_SQL_HOST_NAME')
        postgres_db_name = os.getenv('ECOLLISION_FUSION_SQL_DATABASE_NAME')
        postgres_user = os.getenv('ECOLLISION_FUSION_SQL_USERNAME')
        postgres_password = os.getenv('ECOLLISION_FUSION_SQL_PASSWORD')
        
        postgres_db = PostgreSQLDB(postgres_user, postgres_password, postgres_host, postgres_db_name)

        # Default to all tables if none specified
        if tables is None:
            oracle_tables_query = "SELECT table_name FROM all_tables WHERE owner = 'ECRDBA'"
            headers, tables = oracle_db.query_without_param(oracle_tables_query)

        for table in tables:
            table_name = table if isinstance(table, str) else table[0]
            owner = oracle_db.get_table_owner(table_name)
            columns = oracle_db.get_table_columns(table_name)
            constraints = oracle_db.get_constraints(table_name)

            create_query = create_oracle_table_query(table_name, columns, constraints, dev_mode=dev_mode)
            prefixed_table_name = f"{'oracle_' + table_name}_dev" if dev_mode else f"oracle_{table_name}"

            # Drop existing table if needed
            if drop_existing:
                drop_query = f"DROP TABLE IF EXISTS {prefixed_table_name} CASCADE"
                logging.info(f"Dropping existing table {prefixed_table_name} in PostgreSQL.")
                postgres_db.execute_query(drop_query)

            # Create table in PostgreSQL
            logging.info(f"Creating table {table_name} in PostgreSQL.")
            postgres_db.execute_query(create_query)

            # Fetch and insert data
            data_query = f"SELECT * FROM {owner}.{table_name}" if sample_size is None else f"SELECT * FROM {owner}.{table_name} WHERE ROWNUM <= {sample_size}"
            _, data = oracle_db.query_without_param(data_query)
            insert_query = f"INSERT INTO {prefixed_table_name} ({', '.join([col[0] for col in columns])}) VALUES ({', '.join(['%s'] * len(columns))})"

            for row in data:
                try:
                    postgres_db.execute_query(insert_query, row)
                except Exception as e:
                    logging.error(f"Error inserting row into {prefixed_table_name}: {row}. Error: {e}")

        oracle_db.close_connection()
        postgres_db.close_connection()
        logging.info("Backup operation completed successfully.")
    
    except Exception as e:
        logging.error(f"Backup operation failed: {e}")

if __name__ == "__main__":
    # Specify the tables to backup, or set to None to backup all tables
    # tables_to_backup = None  # Change this to a list of table names to specify, e.g., ['COLLISIONS', 'CL_OBJECTS'] 
    
    # tables_to_backup = ['COLLISIONS', 'CL_OBJECTS', 'CLOBJ_PARTY_INFO', 'CLOBJ_PROPERTY_INFO', 'ECR_COLL_PLOTTING_INFO',
    #                     'CODE_TYPE_VALUES', 'CODE_TYPES', 'CL_STATUS_HISTORY', 'ECR_SYNCHRONIZATION_ACTION',
    #                     'ECR_SYNCHRONIZATION_ACTION_LOG']  # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    
    dev_mode = True  # Set dev_mode to True or False as needed
    drop_existing = True
    sample_size = 888
    # tables_to_backup = ['COLLISIONS', 'CL_OBJECTS', 'CLOBJ_PARTY_INFO', 'CLOBJ_PROPERTY_INFO', 'ECR_COLL_PLOTTING_INFO',
    #                     'CODE_TYPE_VALUES', 'CODE_TYPES', 'CL_STATUS_HISTORY', 'ECR_SYNCHRONIZATION_ACTION',
    #                     'ECR_SYNCHRONIZATION_ACTION_LOG'] # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    tables_to_backup = ['COLLISIONS'] # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    
    backup_oracle_to_postgres(tables=tables_to_backup, sample_size=sample_size, drop_existing=drop_existing, dev_mode=dev_mode)
