# -------------------------------------------------------------  Actions   -------------------------------------------------------------- 
# System Methods : 
def register_system(Ctl_Id, source_system_alias, source_system_name, Path_Name='/'):
    return f"{environment}.EXEC GCFR_Register_System({Ctl_Id},'{source_system_alias}',{Path_Name},'{source_system_name}');"
#---------------------------------------------------------
# Stream Methods :
def register_stream(stream_key,Cycle_Freq_Code, stream_name, Business_Date):
    return f"CALL {environment}.GCFR_UT_Register_Stream({stream_key},{Cycle_Freq_Code}'{stream_name}','{Business_Date}' );"
#---------------------------------------------------------
    
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

#---------------------------------------------------------
