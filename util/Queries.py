# from pages.Filter_and_export_SMX import key_set_names


# environment = "GDEV1M_GCFR"

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
    return (
        f"CREATE SET TABLE {user}.SampleEmployee "
        "(Associate_Id     INTEGER, "
        "Associate_Name   CHAR(25), "
        "Job_Title        VARCHAR(25)) "
        "UNIQUE PRIMARY INDEX (Associate_Id);"
    )

# -------------------------------------------------------------  Actions   -------------------------------------------------------------- 
# System Methods : 
def register_system(Ctl_Id, source_system_alias, source_system_name, Path_Name='/'):
    return f"{environment}.EXEC GCFR_Register_System({Ctl_Id},'{source_system_alias}',{Path_Name},'{source_system_name}');"
#---------------------------------------------------------
# Stream Methods :
def register_stream(stream_key,Cycle_Freq_Code, stream_name, Business_Date):
    return f"CALL {environment}.GCFR_UT_Register_Stream({stream_key},{Cycle_Freq_Code}'{stream_name}','{Business_Date}' );"
#---------------------------------------------------------
# # BKEY Methods :
# def bkey_Process_registeration(key_set_name:int,STAGING_TABLE:str,COLUMN_NAME:str,Domain_Name:int):
#     process_name = f"BK_{key_set_name}_{Domain_Name}_{STAGING_TABLE}_SK{COLUMN_NAME}"
#     return f"""
#     EXEC {environment}.GCFR_Register_Process({process_name},Description,Process_Type,Ctl_Id,Stream_Key, In_DB_Name, {process_name}, Out_DB_Name, Out_Object_Name, Target_TableDatabaseName, Target_TableName, Temp_DatabaseName, {key_set_id}, {Domain_Id}, {Code_Set_Id}, Collect_Stats,Truncate_Target,Verification_Flag,File_Qualifier_Reset_Flag);
#     """
    
def bkey_views(key_set_name ,Domain_Name ,STAGING_TABLE,COLUMN_NAME , environment):

    view_name="BK_"+key_set_name+"_"+Domain_Name+"_"+STAGING_TABLE+"_"+COLUMN_NAME

    return f"""
    REPLACE VIEW {environment}.{view_name} AS LOCK ROW FOR ACCESS 
    SELECT TRIM({COLUMN_NAME}) AS SOURCE_KEY
    FROM {environment}.{STAGING_TABLE}
    WHERE {COLUMN_NAME} IS NOT NULL AND {COLUMN_NAME} <> ''
    GROUP BY 1;

    REPLACE VIEW {environment}.{view_name} AS LOCK ROW FOR ACCESS 
    SELECT REGEXP_REPLACE({COLUMN_NAME}, '[^0-9]', '', 1, 0) AS SOURCE_KEY
    FROM {environment}.{STAGING_TABLE}
    WHERE {COLUMN_NAME} IS NOT NULL AND {COLUMN_NAME} <> ''
    AND LENGTH(REGEXP_REPLACE({COLUMN_NAME}, '[^0-9]', '', 1, 0)) = 11
    GROUP BY 1;

    REPLACE VIEW {environment}.{view_name} AS LOCK ROW FOR ACCESS 
    SELECT TRIM({COLUMN_NAME}) AS SOURCE_KEY
    FROM {environment}.{STAGING_TABLE}
    WHERE {COLUMN_NAME} IS NOT NULL AND {COLUMN_NAME} <> ''
    GROUP BY 1;

    REPLACE VIEW {environment}.{view_name} AS LOCK ROW FOR ACCESS 
    SELECT REGEXP_REPLACE(MOBILE_NUMBER, '[^0-9]', '', 1, 0s) AS SOURCE_KEY
    FROM {environment}.{STAGING_TABLE}
    WHERE EMP_ID IS NOT NULL AND EMP_ID <> ''
    AND LENGTH(REGEXP_REPLACE(MOBILE_NUMBER, '[^0-9]', '', 1, 0)) = 11
    GROUP BY 1;
    """

def bkey_call(Process_Name:str):
        return f"CALL {environment}.GCFR_PP_BKEY({Process_Name},6,01,02)";
#---------------------------------------------------------
