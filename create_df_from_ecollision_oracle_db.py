import cx_Oracle
import pandas as pd
from dotenv import load_dotenv
import os

# load secrets
load_dotenv()
username = os.getenv('ECOLLISION_ORACLE_SQL_USERNAME')
user_password = os.getenv('ECOLLISION_ORACLE_SQL_PASSWORD')

# control panel
n_row_to_print = 1000
query_city = 'edmonton' # options: 'edmonton', 'calgary'
query_agg = False # options: True, False
print_switch = False

# select the sql query to execute; options below:
base_path = 'M:\\SPE\\OTS\\Stats-OTS\\Kai\\git_repo\\ecollision_analytics_assessment\\ecollision-analytics-assessment\\traffic_dashboard\\'

# sql_query_to_execute = 'traffic_dashboard/test_query.sql'
# sql_query_to_execute = f'traffic_dashboard/test_ecollision_oracle_for_analytics_v5_city={query_city}_agg={str(query_agg).lower()}.sql'
sql_query_to_execute_edmonton_not_agg = os.path.join(base_path, f'query_ecollision_oracle_for_analytics_v5_city=edmonton_agg=false.sql')
sql_query_to_execute_calgary_not_agg = os.path.join(base_path, f'query_ecollision_oracle_for_analytics_v5_city=calgary_agg=false.sql')
sql_query_to_execute_edmonton_agg = os.path.join(base_path, f'query_ecollision_oracle_for_analytics_v5_city=edmonton_agg=true.sql')
sql_query_to_execute_calgary_agg = os.path.join(base_path, f'query_ecollision_oracle_for_analytics_v5_city=calgary_agg=true.sql')

# set up for Oracle SQL db connection
oracle_instant_client_dir = 'C:\\Users\\kai.wong\\_local_dev\\oracle_instant_client\\instantclient-basic-windows.x64-23.4.0.24.05\\instantclient_23_4'
cx_Oracle.init_oracle_client(lib_dir=oracle_instant_client_dir)

db_host = 'ncc-production-qf14q-scan.exacc.ds.gov.ab.ca'
db_port = 1521
db_service_name = 'AT12PRD.WORLD'

conn_info = {
    'host': db_host,
    'port': db_port,
    'user': username,
    'psw': user_password,
    'service': db_service_name
}

conn_str = '{user}/{psw}@//{host}:{port}/{service}'.format(**conn_info)

class DB:
    def __init__(self):
        self.conn = cx_Oracle.connect(conn_str)

    def query_with_param(self, query, params=None):
        cursor = self.conn.cursor()
        result = cursor.execute(query, params).fetchall()
        header = [i[0] for i in cursor.description]
        cursor.close()
        return header, result
    
    def query_without_param(self, query):
        cursor = self.conn.cursor()
        result = cursor.execute(query).fetchall()
        header = [i[0] for i in cursor.description]
        cursor.close()
        return header, result
    
    def load_query_from_file(self, file_path):
        with open(file_path, 'r') as file:
            return file.read()
    
    def close_connection(self):
        self.conn.close()

######
# connect to Oracle SQL db, and create df_edmonton_agg
db = DB()
sql_query_edmonton_agg = db.load_query_from_file(sql_query_to_execute_edmonton_agg)
result_edmonton_agg = db.query_without_param(sql_query_edmonton_agg)
db.close_connection()

# convert the query result to a Pandas DataFrame
header, data = result_edmonton_agg[0], result_edmonton_agg[1]
df_edmonton_agg = pd.DataFrame(data, columns=header)
df_edmonton_agg.rename(columns={'TABLENAME': 'TABLE_NAME', 'TABLEORDER': 'TABLE_ORDER'}, inplace=True)
df_edmonton_agg['CITY'] = 'Edmonton'

# connect to Oracle SQL db, and create df_calgary_agg
db = DB()
sql_query_calgary_agg = db.load_query_from_file(sql_query_to_execute_calgary_agg)
result_calgary_agg = db.query_without_param(sql_query_calgary_agg)
db.close_connection()

# convert the query result to a Pandas DataFrame
header, data = result_calgary_agg[0], result_calgary_agg[1]
df_calgary_agg = pd.DataFrame(data, columns=header)
df_calgary_agg.rename(columns={'TABLENAME': 'TABLE_NAME', 'TABLEORDER': 'TABLE_ORDER'}, inplace=True)
df_calgary_agg['CITY'] = 'Calgary'

# mrege df_edmonton_agg and df_calgary_agg
df_agg = pd.concat([df_edmonton_agg, df_calgary_agg], ignore_index=True)
df_agg.reset_index(drop=True, inplace=True)

if __name__ == '__main__':
    print(df_agg.head())
    print(df_agg.tail())
    