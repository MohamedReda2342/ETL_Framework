import teradatasql
import pandas as pd
from dotenv import load_dotenv
import os
from teradataml import *
from util import Queries

# Load the .env file with error handling
try:
    # Check if .env file exists before loading
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    if not os.path.exists(env_path):
        print(f"Warning: .env file not found at {env_path}")
    
    # Attempt to load the .env file
    load_dotenv()
except Exception as e:
    print(f"Error loading .env file: {str(e)}")

# Function to get connection parameters from environment variables
def get_connection_params():
    # Get environment variables with validation
    user = os.getenv("TD_DB_USER")
    password = os.getenv("TD_DB_PASSWORD")
    host = os.getenv("TD_DB_HOST")
    
    # Validate that all required parameters are present
    missing_params = []
    if not user:
        missing_params.append("TD_DB_USER")
    if not password:
        missing_params.append("TD_DB_PASSWORD")
    if not host:
        missing_params.append("TD_DB_HOST")
    
    if missing_params:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_params)}. Please check your .env file.")
    
    return {
        "user": user,
        "password": password,
        "host": host
    }

# Function to establish a connection to Teradata
def get_connection():
    try:
        params = get_connection_params()
        return teradatasql.connect(
            host=params["host"], 
            user=params["user"], 
            password=params["password"]
        )
    except ValueError as ve:
        # Re-raise configuration errors
        raise ve
    except Exception as e:
        # Handle connection errors
        error_msg = str(e)
        if "Hostname lookup failed" in error_msg:
            raise ConnectionError(f"Failed to connect to Teradata host '{params.get('host')}'. Please verify the hostname is correct.")
        elif "Invalid username or password" in error_msg:
            raise ConnectionError("Authentication failed. Please check your username and password in the .env file.")
        else:
            raise ConnectionError(f"Failed to connect to Teradata: {error_msg}")

# Function to execute a query and return results as a DataFrame
def execute_query(query):
    try:
        with get_connection() as connect:
            result_df = pd.read_sql(query, connect)
            print(f"Query executed successfully: {query}")
            return result_df
    except (ValueError, ConnectionError) as config_error:
        # Configuration errors should be propagated
        print(f"Configuration error: {str(config_error)}")
        raise config_error
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise e

def establish_TD_connection():
    query = Queries.get_distinct_databases()
    print(query)
    try:
        data = execute_query(query)
        print(data)
        return data, query
    except Exception as e:
        print(f"Error in establish_TD_connection: {str(e)}")
        raise e

def list_objects(database_name='TDStats'):
    query = Queries.list_objects_by_database(database_name=database_name)
    try:
        tables = execute_query(query)
        print(tables)
        return tables, query
    except Exception as e:
        print(f"Error in list_objects: {str(e)}")
        raise e

def create_core_tables(sql_script):
    for query in sql_script:
        try:
            response = execute_query(query)
            return response
        except teradatasql.DatabaseError as db_err:
            return db_err

def create_core(sql_script):
    params = get_connection_params()
    db_user_for_table = params["user"]
    
    create_table_query = Queries.create_sample_employee_table(user=db_user_for_table)

    try:
        with get_connection() as con:
            with con.cursor() as cur:
                try:
                    cur.execute(create_table_query)
                    print(f"Sample table {db_user_for_table}.SampleEmployee created.")
                except teradatasql.DatabaseError as db_err:
                    print("Error while executing the query:", db_err)
    except teradatasql.DatabaseError as db_err:
        print("Error while connecting to the Teradata database:", db_err)

def execute_custom_query(query_string):
    try:
        result_df = execute_query(query_string)
        return result_df, query_string
    except Exception as e:
        print(f"Error executing custom query: {str(e)}")
        raise e

# For backward compatibility
def create_table(script):
    print(script)
