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
#         # String types
#         'varchar': 'VARCHAR',
#         'nvarchar': 'VARCHAR',            # SQL Server nvarchar
#         'char': 'CHAR',
#         'nchar': 'CHAR',
#         'text': 'TEXT',
#         'ntext': 'TEXT',

#         # Integer types
#         'int': 'INTEGER',
#         'smallint': 'SMALLINT',
#         'tinyint': 'SMALLINT',            # PostgreSQL does not have TINYINT; SMALLINT is closest
#         'bigint': 'BIGINT',

#         # Decimal and float types
#         'decimal': 'DECIMAL',
#         'numeric': 'NUMERIC',
#         'float': 'DOUBLE PRECISION',
#         'real': 'REAL',                   # SQL Server REAL -> PostgreSQL REAL
        
#         # Date and time types
#         'datetime': 'TIMESTAMP',
#         'datetime2': 'TIMESTAMP',         # SQL Server datetime2 -> PostgreSQL TIMESTAMP
#         'smalldatetime': 'TIMESTAMP',     # SQL Server smalldatetime -> PostgreSQL TIMESTAMP
#         'date': 'DATE',
#         'time': 'TIME',

#         # Boolean types
#         'bit': 'BOOLEAN',                 # SQL Server bit -> PostgreSQL BOOLEAN

#         # Binary types
#         'binary': 'BYTEA',
#         'varbinary': 'BYTEA',
#         'image': 'BYTEA',

#         # Other types
#         'uniqueidentifier': 'UUID',       # SQL Server uniqueidentifier -> PostgreSQL UUID
#         'xml': 'XML',
#         'money': 'NUMERIC',               # SQL Server money -> PostgreSQL NUMERIC
#         'smallmoney': 'NUMERIC',          # SQL Server smallmoney -> PostgreSQL NUMERIC
#     }

#     # Default to TEXT if the type is not mapped
#     pg_type = mapping.get(data_type.lower(), 'TEXT')  
#     logging.debug(f"Mapping MS SQL Server type '{data_type}' to PostgreSQL type '{pg_type}'")
#     return pg_type

def create_analytics_table_query(table_name, columns, constraints, dev_mode=False):
    # Prefix the table name with 'analytics_' and add '_dev' suffix if dev_mode is enabled
    suffix = "_dev" if dev_mode else ""
    prefixed_table_name = f"analytics_{table_name}{suffix}"
    column_defs = []
    primary_key_column = ecollision_analytics_db_table_primary_key.get(table_name)

    for column in columns:
        col_name = column[0]
        data_type = map_analytics_db_to_postgres(column[1])
        nullable = 'NOT NULL' if column[3] == 'NO' else ''  # Only append NOT NULL if applicable
        column_defs.append(f"{col_name} {data_type} {nullable}".strip())  # Strip to avoid extra spaces

    # Add primary key constraint if available
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
def backup_analytics_to_postgres(tables=None, sample_size=None, batch_size=100, drop_existing=False, dev_mode=False):
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
            
            # Drop existing table if the option is enabled
            suffix = "_dev" if dev_mode else ""
            prefixed_table_name = f"analytics_{table_name}{suffix}"
            if drop_existing:
                drop_query = f"DROP TABLE IF EXISTS {prefixed_table_name} CASCADE;"
                try:
                    logging.debug(f"Dropping existing table: {prefixed_table_name}")
                    postgres_db.execute_query(drop_query)
                except Exception as e:
                    logging.error(f"Failed to drop table {table_name}: {e}")
                    continue
            
            columns = analytics_db.get_table_columns(table_name)
            constraints = analytics_db.get_constraints(table_name)
            create_query = create_analytics_table_query(table_name, columns, constraints, dev_mode=dev_mode)

            try:
                logging.debug(f"Executing create table query for {table_name}.")
                postgres_db.execute_query(create_query)
            except Exception as e:
                logging.error(f"Failed to create table {table_name}: {e}")
                continue

            select_query = f"SELECT TOP {sample_size} * FROM [eCollisionAnalytics].[ECRDBA].{table_name}" if sample_size else f"SELECT * FROM [eCollisionAnalytics].[ECRDBA].{table_name}"
            
            logging.debug(f"Selecting data from {table_name}. Query: {select_query}")
            header, data = analytics_db.query_without_param(select_query)

            insert_query = f"INSERT INTO {prefixed_table_name} ({', '.join(header)}) VALUES ({', '.join(['%s'] * len(header))})"
            
            batch = []
            for i, row in enumerate(data):
                batch.append(row)
                if len(batch) == batch_size:
                    try:
                        logging.debug(f"Inserting batch of {batch_size} rows into {table_name}.")
                        postgres_db.batch_insert(insert_query, batch)
                    except Exception as e:
                        logging.error(f"Failed to insert batch into {table_name}. Error: {e}")
                    batch = []

            # Insert remaining rows in the last batch if not empty
            if batch:
                try:
                    logging.debug(f"Inserting final batch of {len(batch)} rows into {table_name}.")
                    postgres_db.batch_insert(insert_query, batch)
                except Exception as e:
                    logging.error(f"Failed to insert final batch into {table_name}. Error: {e}")

        # Closing connections
        logging.debug("Closing database connections.")
        analytics_db.close_connection()
        postgres_db.close_connection()
        logging.info("Backup operation completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the backup process: {e}")
        
if __name__ == "__main__":
    # tables_to_backup = ['COLLISIONS', 'CL_OBJECTS', 'CLOBJ_PARTY_INFO', 'CLOBJ_PROPERTY_INFO', 'ECR_COLL_PLOTTING_INFO',
    #                     'CODE_TYPE_VALUES', 'CODE_TYPES', 'CL_STATUS_HISTORY', 'ECR_SYNCHRONIZATION_ACTION_ETL',
    #                     'ECR_SYNCHRONIZATION_ACTION_LOG_ETL']  # Change this to a list of table names to specify, e.g., ['COLLISIONS']
    
    # tables_to_backup = ['CODE_TYPES']
    
    dev_mode = True
    drop_existing = True
    # tables_to_backup = ['COLLISIONS', 'CL_OBJECTS', 'CLOBJ_PARTY_INFO', 'CLOBJ_PROPERTY_INFO', 'ECR_COLL_PLOTTING_INFO',
    #                     'CODE_TYPE_VALUES', 'CODE_TYPES', 'CL_STATUS_HISTORY', 'ECR_SYNCHRONIZATION_ACTION_ETL',
    #                     'ECR_SYNCHRONIZATION_ACTION_LOG_ETL']
    tables_to_backup = ['COLLISIONS']
    sample_size = 888
    batch_size = None
    
    # Enable dev_mode to use _dev table suffix
    backup_analytics_to_postgres(tables=tables_to_backup, sample_size=sample_size, batch_size=batch_size, 
                                 drop_existing=drop_existing, dev_mode=dev_mode)