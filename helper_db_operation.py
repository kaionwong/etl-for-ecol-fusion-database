import os
import psycopg2
import cx_Oracle
import psycopg2
import psycopg2.extras
import pyodbc
import logging

class OracleDB:
    def __init__(self, username, password, db_host, db_port, db_service):
        # Oracle Instant Client setup
        oracle_instant_client_dir = os.getenv('ORACLE_INSTANT_CLIENT_DIR')
        cx_Oracle.init_oracle_client(lib_dir=oracle_instant_client_dir)

        self.conn_str = f"{username}/{password}@//{db_host}:{db_port}/{db_service}"
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
        constraint = self.query_without_param(query)[1]
        logging.debug(f"Constraint content: {constraint}")
        return constraint

    def get_table_owner(self, table_name):
        query = f"""
        SELECT owner 
        FROM all_tables 
        WHERE table_name = '{table_name.upper()}'
        """
        logging.debug(f"Getting owner for table: {table_name}")
        return self.query_without_param(query)[1][0][0]  # Returns the owner

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

    def batch_insert(self, query, data_batch):
        cursor = self.conn.cursor()
        try:
            logging.debug(f"Executing batch insert with {len(data_batch)} rows.")
            psycopg2.extras.execute_batch(cursor, query, data_batch)
            self.conn.commit()
            logging.debug(f"Batch insert committed successfully with {len(data_batch)} rows.")
        except Exception as e:
            logging.error(f"Batch insert failed. Error: {e}")
            self.conn.rollback()
            logging.debug("Transaction rolled back due to error in batch insert.")
            raise
        finally:
            cursor.close()

    def close_connection(self):
        logging.debug("Closing PostgreSQL DB connection.")
        self.conn.close()

def map_analytics_db_to_postgres(data_type):
    """ Map MS SQL Server types to PostgreSQL data types """
    mapping = {
        'varchar': 'VARCHAR',
        'nvarchar': 'VARCHAR',
        'char': 'CHAR',
        'nchar': 'CHAR',
        'text': 'TEXT',
        'ntext': 'TEXT',
        'int': 'INTEGER',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',
        'bigint': 'BIGINT',
        'decimal': 'DECIMAL',
        'numeric': 'NUMERIC',
        'float': 'DOUBLE PRECISION',
        'real': 'REAL',
        'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP',
        'smalldatetime': 'TIMESTAMP',
        'date': 'DATE',
        'time': 'TIME',
        'bit': 'BOOLEAN',
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
        'image': 'BYTEA',
        'uniqueidentifier': 'UUID',
        'xml': 'XML',
        'money': 'NUMERIC',
        'smallmoney': 'NUMERIC',
    }
    return mapping.get(data_type.lower(), 'TEXT')

def map_oracle_to_postgres(data_type):
    """ Map Oracle data types to PostgreSQL data types """
    mapping = {
        # String types
        'VARCHAR2': 'VARCHAR',
        'NVARCHAR2': 'VARCHAR',            # Oracle NVARCHAR2 -> PostgreSQL VARCHAR
        'CHAR': 'CHAR',
        'NCHAR': 'CHAR',
        'CLOB': 'TEXT',                    # Oracle CLOB -> PostgreSQL TEXT
        'NCLOB': 'TEXT',                   # Oracle NCLOB -> PostgreSQL TEXT

        # Numeric types
        'NUMBER': 'NUMERIC',               # Oracle NUMBER -> PostgreSQL NUMERIC (with precision support)
        'BINARY_FLOAT': 'REAL',            # Oracle BINARY_FLOAT -> PostgreSQL REAL
        'BINARY_DOUBLE': 'DOUBLE PRECISION', # Oracle BINARY_DOUBLE -> PostgreSQL DOUBLE PRECISION
        'FLOAT': 'DOUBLE PRECISION',
        'INTEGER': 'INTEGER',
        'SMALLINT': 'SMALLINT',
        
        # Date/Time types
        'DATE': 'TIMESTAMP',               # Oracle DATE -> PostgreSQL TIMESTAMP
        'TIMESTAMP': 'TIMESTAMP',          # Oracle TIMESTAMP -> PostgreSQL TIMESTAMP
        'TIMESTAMP WITH TIME ZONE': 'TIMESTAMPTZ', # Oracle TIMESTAMP WITH TIME ZONE -> PostgreSQL TIMESTAMPTZ
        'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMPTZ', # Similar handling

        # Boolean type
        'BOOLEAN': 'BOOLEAN',              # PostgreSQL has native BOOLEAN support (Oracle does not)

        # Binary types
        'BLOB': 'BYTEA',                   # Oracle BLOB -> PostgreSQL BYTEA
        'RAW': 'BYTEA',                    # Oracle RAW -> PostgreSQL BYTEA
        'LONG RAW': 'BYTEA',               # Oracle LONG RAW -> PostgreSQL BYTEA

        # Other types
        'ROWID': 'TEXT',                   # Oracle ROWID -> PostgreSQL TEXT
        'UROWID': 'TEXT',                  # Oracle UROWID -> PostgreSQL TEXT
        'XMLTYPE': 'XML',                  # Oracle XMLTYPE -> PostgreSQL XML
        'LONG': 'TEXT',                    # Oracle LONG -> PostgreSQL TEXT
    }

    # Default to TEXT if the type is not mapped
    pg_type = mapping.get(data_type.upper(), 'TEXT')  
    logging.debug(f"Mapping Oracle type '{data_type}' to PostgreSQL type '{pg_type}'")
    return pg_type