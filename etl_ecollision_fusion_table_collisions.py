import pandas as pd
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

from reference import ecollision_analytics_db_table_primary_key
from helper import time_execution, set_pandas_display_options
from helper_db_operation import AnalyticsDB, OracleDB, PostgreSQLDB, map_analytics_db_to_postgres

# Set up logging configuration
logging.basicConfig(level=logging.CRITICAL, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
set_pandas_display_options()

# Control panel
dev_mode = True
drop_existing = True

# Helper functions
def extract_case_year(df):
    """
    Extracts the year for the `case_year` column. Prefers `occurence_timestamp` if available,
    otherwise uses `reported_timestamp`. If both are missing, sets `case_year` to None.
    """
    df['case_year'] = df['occurence_timestamp'].fillna(df['reported_timestamp'])
    df['case_year'] = pd.to_datetime(df['case_year'], errors='coerce').dt.year
    return df

###########################
###########################
###########################
# 1) ETL Collisions table for Fusion
# 1.1) connect and get the set of valid collision IDs from vw_valid_collision_from_oracle
# Connect to PostgreSQL
postgres_host = os.getenv('ECOLLISION_FUSION_SQL_HOST_NAME')
postgres_db_name = os.getenv('ECOLLISION_FUSION_SQL_DATABASE_NAME')
postgres_user = os.getenv('ECOLLISION_FUSION_SQL_USERNAME')
postgres_password = os.getenv('ECOLLISION_FUSION_SQL_PASSWORD')

logging.debug("Connecting to PostgreSQL DB.")
postgres_db = PostgreSQLDB(postgres_user, postgres_password, postgres_host, postgres_db_name)

# Query to fetch valid collision IDs from Oracle view
sql_query_get_valid_collision_case_from_oracle = """
    SELECT collision_id
    FROM vw_valid_collision_from_oracle
"""

# Execute query and load results into a Pandas DataFrame
try:
    logging.debug("Fetching valid collisions from Oracle view into DataFrame.")
    df_oracle_valid_cases = pd.read_sql(sql_query_get_valid_collision_case_from_oracle, postgres_db.conn)
    logging.debug(f"Fetched {len(df_oracle_valid_cases)} rows of valid collisions.")
except Exception as e:
    logging.error(f"Error while fetching data from Oracle view: {e}")
    raise

# 1.2) connect oracle_collisions, then apply the filter from 1.1) to exclude invalid collisions
# Query to fetch all collisions from Oracle table
sql_query_get_collisions_table_from_oracle = """
    SELECT *
    FROM public.oracle_collisions
"""

# Execute query and load results into a Pandas DataFrame
try:
    logging.debug("Fetching all collisions from oracle_collisions into DataFrame.")
    df_oracle_collisions_table = pd.read_sql(sql_query_get_collisions_table_from_oracle, postgres_db.conn)
    logging.debug(f"Fetched {len(df_oracle_collisions_table)} rows of valid collisions.")
except Exception as e:
    logging.error(f"Error while fetching data from Oracle view: {e}")
    raise

# Apply the filter to include only those collisions that are in the valid list from df_oracle_valid_cases
valid_collision_ids = df_oracle_valid_cases['collision_id'].tolist()

# Filter df_oracle_collisions_table to include only rows where the ID is in the valid collision IDs
df_oracle_collisions_table_filtered = df_oracle_collisions_table[df_oracle_collisions_table['id'].isin(valid_collision_ids)]

# Check the result of the filtering
logging.debug(f"Filtered {len(df_oracle_collisions_table_filtered)} valid collisions.")

# 1.3) format fields to match that of eCollision Analytics (according to supplementary/column_mapping_btw_analytics_and_oracle_tables.xlsx)
# Apply the function to calculate case_year
df_oracle_collisions_table_filtered = extract_case_year(df_oracle_collisions_table_filtered)

# Change column name from 'fatal_comment' to 'fatal_comments'
df_oracle_collisions_table_filtered = df_oracle_collisions_table_filtered.rename(columns={'fatal_comment': 'fatal_comments'})

# Create column occurence_timestring that extracts year/month/day from occurence_timestamp
df_oracle_collisions_table_filtered['occurence_timestring'] = df_oracle_collisions_table_filtered['occurence_timestamp'].dt.strftime('%Y-%m-%d')

# Add a "source" column with the value "eCollision Oracle"
df_oracle_collisions_table_filtered['source'] = "eCollision Oracle"

# 1.4) import into Fusion's Collisions table
# Determine the target table based on dev_mode
target_table = 'fusion_collisions_dev' if dev_mode else 'fusion_collisions'

# Fetch the target table schema to determine the column names dynamically
try:
    query_table_schema = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{target_table}';
    """
    df_table_schema = pd.read_sql(query_table_schema, postgres_db.conn)
    target_columns = df_table_schema['column_name'].tolist()
    logging.debug(f"Fetched {len(target_columns)} columns for the target table {target_table}.")
except Exception as e:
    logging.error(f"Error fetching schema for table {target_table}: {e}")
    raise

# Dynamically match columns between the DataFrame and the target table
# Select only the columns present in both the DataFrame and the table
df_for_insertion = df_oracle_collisions_table_filtered[
    [col for col in df_oracle_collisions_table_filtered.columns if col in target_columns]
]

# If drop_existing is True, delete the existing content of the table
if drop_existing:
    try:
        delete_query = f"DELETE FROM {target_table};"
        postgres_db.execute_query(delete_query)
        logging.debug(f"Deleted existing content in the table: {target_table}")
    except Exception as e:
        logging.error(f"Error while deleting content from the table {target_table}: {e}")
        raise

# Insert the filtered and dynamically mapped data into PostgreSQL
try:
    postgres_db.bulk_insert_dataframe(df_for_insertion, target_table)
    logging.info(f"Successfully imported {len(df_for_insertion)} rows into {target_table}.")
except Exception as e:
    logging.error(f"Error while inserting data into table {target_table}: {e}")
    raise
