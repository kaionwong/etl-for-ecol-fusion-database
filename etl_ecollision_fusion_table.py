# 1) For the Collisions table, 
# 1.1) use vw_valid_collision_from_oracle, 
# 1.2) apply this as a filter to include only those collisions that are on the valid list, 
# 1.3) format fields to match that of eCollision Analytics (according to supplementary/column_mapping_btw_analytics_and_oracle_tables.xlsx), 
# 1.4) import into Fusion's Collisions table

# 2) For the CL_OBJECTS table,
# 2.1) remove all the records that have COLLISION_ID in CL_OBJECTS that are not in the ID column of Collisions table
# 2.2) perform column reformatting
# 2.3) import into Fusion's CL_OBJECTS

# 3) For the CL_PARTY_INFO table,
# 3.1) remove all the records that have ID in CLOBJ_PARTY_INFO that are not in the PARTY_ID column of CL_OIBJECTS table
# 3.2) perform column reformatting
# 3.3) import into Fusion's CL_PARTY_INFO

# 4) For the CLOBJ_PROPERTY_INFO table,
# 4.1) remove all the records that have ID in CLOBJ_PROPERTY_INFO that are not in the PROPERTY_ID column of CL_OIBJECTS table
#   AND(!!!) ID in CLOBJ_PROPERTY_INFO that are not in the OPERATED_PROPERTY_ID column of CLOBJ_PARTY_INFO table
# 4.2) perform column reformatting
# 4.3) import into Fusion's CLOBJ_PROPERTY_INFO

# 5) For all the rest of the tables, do the re-formating that's neccesary to match the oracle format to the fusion/analytics format

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
dev_mode = False
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

# Query to fetch valid collision IDs from Oracle view (assuming PostgreSQL is also handling Oracle data)
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
# Database connection setup

# Query to fetch valid collision IDs from Oracle view (assuming PostgreSQL is also handling Oracle data)
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

# 1.3) format fields to match that of eCollision Analytics (according to supplementary/column_mapping_btw_analytics_and_oracle_tables.xlsx), 
# create case_year variable
# Apply the function to calculate case_year
df_oracle_collisions_table_filtered = extract_case_year(df_oracle_collisions_table_filtered)

# Chnage column name from 'fatal_comment' to 'fatal_comments'
df_oracle_collisions_table_filtered = df_oracle_collisions_table_filtered.rename(columns={'fatal_comment': 'fatal_comments'})

# Create column occurence_timestring that extracts year/month/day from occurence_timestamp
df_oracle_collisions_table_filtered['occurence_timestring'] = df_oracle_collisions_table_filtered['occurence_timestamp'].dt.strftime('%Y-%m-%d')

print(df_oracle_collisions_table_filtered.head())
print(df_oracle_collisions_table_filtered.tail())

# 1.4) import into Fusion's Collisions table
