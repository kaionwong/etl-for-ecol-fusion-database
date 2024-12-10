# This creates all the tables for eCollision Fusion DB. The specification and formats of these tables will be the same as eCollision Analytics DB's.
# For the Collisions table, 1) use vw_valid_collision_from_oracle, 2) apply this as a filter to include only those collisions that are on the valid list, 
# 3) format fields to match that of eCollision Analytics (according to supplementary/column_mapping_btw_analytics_and_oracle_tables.xlsx), 
# 4) import into Fusion's Collisions table
# For all other tables, do the same except there is no need to apply the filter on the Collisions table

import pandas as pd
from dotenv import load_dotenv
import os
import logging

from reference import ecollision_analytics_db_table_primary_key
from helper import time_execution
from helper_db_operation import AnalyticsDB, PostgreSQLDB, map_analytics_db_to_postgres

# Set up logging configuration
logging.basicConfig(level=logging.CRITICAL, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

# def map_analytics_db_to_postgres(data_type):
#     """ Map MS SQL Server types to PostgreSQL data types """
#     mapping = {
#         'varchar': 'VARCHAR',
#         'nvarchar': 'VARCHAR',
#         'char': 'CHAR',
#         'nchar': 'CHAR',
#         'text': 'TEXT',
#         'ntext': 'TEXT',
#         'int': 'INTEGER',
#         'smallint': 'SMALLINT',
#         'tinyint': 'SMALLINT',
#         'bigint': 'BIGINT',
#         'decimal': 'DECIMAL',
#         'numeric': 'NUMERIC',
#         'float': 'DOUBLE PRECISION',
#         'real': 'REAL',
#         'datetime': 'TIMESTAMP',
#         'datetime2': 'TIMESTAMP',
#         'smalldatetime': 'TIMESTAMP',
#         'date': 'DATE',
#         'time': 'TIME',
#         'bit': 'BOOLEAN',
#         'binary': 'BYTEA',
#         'varbinary': 'BYTEA',
#         'image': 'BYTEA',
#         'uniqueidentifier': 'UUID',
#         'xml': 'XML',
#         'money': 'NUMERIC',
#         'smallmoney': 'NUMERIC',
#     }
#     return mapping.get(data_type.lower(), 'TEXT')

def create_fusion_table_query(table_name, columns, constraints, dev_mode=False):
    # Use the "fusion_" prefix instead of "analytics_"
    suffix = "_dev" if dev_mode else ""
    prefixed_table_name = f"fusion_{table_name}{suffix}"
    column_defs = []
    primary_key_column = ecollision_analytics_db_table_primary_key.get(table_name)

    for column in columns:
        col_name = column[0]
        data_type = map_analytics_db_to_postgres(column[1])
        nullable = 'NOT NULL' if column[3] == 'NO' else ''
        column_defs.append(f"{col_name} {data_type} {nullable}".strip())

    if primary_key_column:
        column_defs.append(f"PRIMARY KEY ({primary_key_column})")

    create_query = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{prefixed_table_name.lower()}') THEN
            CREATE TABLE {prefixed_table_name} ({', '.join(column_defs)});
        END IF;
    END $$;
    """
    
    logging.debug(f"Generated CREATE TABLE query for {prefixed_table_name}: {create_query}")
    return create_query

@time_execution
def create_empty_fusion_tables_in_postgres(tables=None, dev_mode=False, drop_existing=False):
    try:
        logging.info("Starting operation to create empty tables in PostgreSQL.")

        # Connect to AnalyticsDB
        analytics_db_driver = os.getenv('ECOLLISION_ANALYTICS_SQL_DRIVER')
        analytics_db_server = os.getenv('ECOLLISION_ANALYTICS_SQL_SERVER').replace('\\\\', '\\')
        analytics_db_name = os.getenv('ECOLLISION_ANALYTICS_SQL_DATABASE_NAME')
        analytics_db_trusted_connection = os.getenv('ECOLLISION_ANALYTICS_SQL_TRUSTED_CONNECTION')
        analytics_db = AnalyticsDB(analytics_db_name, analytics_db_server, analytics_db_driver, analytics_db_trusted_connection)

        # Connect to PostgreSQL
        postgres_host = os.getenv('ECOLLISION_FUSION_SQL_HOST_NAME')
        postgres_db_name = os.getenv('ECOLLISION_FUSION_SQL_DATABASE_NAME')
        postgres_user = os.getenv('ECOLLISION_FUSION_SQL_USERNAME')
        postgres_password = os.getenv('ECOLLISION_FUSION_SQL_PASSWORD')
        postgres_db = PostgreSQLDB(postgres_user, postgres_password, postgres_host, postgres_db_name)

        if tables is None:
            analytics_db_tables_query = """
            SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
            """
            headers, tables = analytics_db.query_without_param(analytics_db_tables_query)

        if not tables:
            logging.warning("No tables found in the Analytics DB.")
            return

        for table in tables:
            table_name = table if isinstance(table, str) else table[0]
            logging.debug(f"Processing table: {table_name}")

            # Drop existing table if the option is enabled
            suffix = "_dev" if dev_mode else ""
            prefixed_table_name = f"fusion_{table_name}{suffix}"
            if drop_existing:
                drop_query = f"DROP TABLE IF EXISTS {prefixed_table_name} CASCADE;"
                try:
                    logging.debug(f"Dropping existing table: {prefixed_table_name}")
                    postgres_db.execute_query(drop_query)
                except Exception as e:
                    logging.error(f"Failed to drop table {table_name}: {e}")
                    continue

            # Fetch columns and constraints
            columns = analytics_db.get_table_columns(table_name)
            constraints = analytics_db.get_constraints(table_name)
            create_query = create_fusion_table_query(table_name, columns, constraints, dev_mode=dev_mode)

            try:
                logging.debug(f"Executing create table query for {table_name}.")
                postgres_db.execute_query(create_query)
            except Exception as e:
                logging.error(f"Failed to create table {table_name}: {e}")

        # Closing connections
        logging.debug("Closing database connections.")
        analytics_db.close_connection()
        postgres_db.close_connection()
        logging.info("Table creation operation completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the table creation process: {e}")
        
if __name__ == "__main__":
    dev_mode = False
    drop_existing = True
    tables_to_create = ['COLLISIONS', 'CL_OBJECTS', 'CLOBJ_PARTY_INFO', 'CLOBJ_PROPERTY_INFO', 'ECR_COLL_PLOTTING_INFO',
                         'CODE_TYPE_VALUES', 'CODE_TYPES', 'CL_STATUS_HISTORY', 'ECR_SYNCHRONIZATION_ACTION_ETL',
                         'ECR_SYNCHRONIZATION_ACTION_LOG_ETL']
    create_empty_fusion_tables_in_postgres(tables=tables_to_create, dev_mode=dev_mode, drop_existing=drop_existing)
