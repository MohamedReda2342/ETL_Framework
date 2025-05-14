"""
Provides SQL queries used throughout the application.
"""

def get_distinct_databases():
    """
    Query to select distinct database names from DBC.TablesV.
    """
    return "SELECT DISTINCT DatabaseName FROM DBC.TablesV"

def list_objects_by_database(database_name: str):
    """
    Query to select table information for a specific database.
    Args:
        database_name (str): The name of the database.
    """
    return f"SELECT DatabaseName, TableName, CreateTimeStamp, LastAlterTimeStamp FROM DBC.TablesV WHERE DatabaseName = '{database_name}'"

def create_sample_employee_table(user: str):
    """
    Query to create a sample employee table.
    Args:
        user (str): The user or schema name for the table.
    """
    return (
        f"CREATE SET TABLE {user}.SampleEmployee "
        "(Associate_Id     INTEGER, "
        "Associate_Name   CHAR(25), "
        "Job_Title        VARCHAR(25)) "
        "UNIQUE PRIMARY INDEX (Associate_Id);"
    )

def register_system(source_system_id, source_system_alias, source_system_name):
    """
    Query to register a system.
    Example: EXEC GCFR_Register_System( '1','/','CB','Core Banking' );
    Assumes string parameters will be quoted appropriately by the calling function or are passed pre-quoted if needed by concat_util.
    For direct use, ensure string parameters are passed with quotes if the SQL syntax requires them.
    The original code used a utility (cu.concat_4) which likely handled quoting.
    If parameters are numeric, they usually don't need quotes.
    """
    # This version assumes parameters are passed ready for concatenation
    # or that the calling code will handle quoting as per cu.concat_4's behavior.
    # For simplicity, if direct string values are expected by SQL, they should be quoted.
    # Example: EXEC GCFR_Register_System( 'id_val', '/', 'alias_val', 'name_val' );
    return f"EXEC GCFR_Register_System( '{source_system_id}', '/', '{source_system_alias}', '{source_system_name}' );"


def register_stream(system_id, stream_key, stream_name, date_str):
    """
    Query to register a stream.
    Example: CALL GCFR_UT_Register_Stream( 1,1,'CB_STG','2025-04-13' );
    Assumes system_id and stream_key are numeric and stream_name, date_str are strings that need quoting.
    The original code used a utility (cu.concat_2) for stream_name and date_str which likely handled quoting.
    """
    return f"CALL GCFR_UT_Register_Stream( {system_id},{stream_key},'{stream_name}','{date_str}' );"