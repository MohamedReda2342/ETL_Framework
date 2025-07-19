import math
import streamlit as st
from sqlalchemy import column, desc

# -------------------------------------------------------------  Actions   -------------------------------------------------------------- 
def generate_bkey_views(smx_model, env):
    # Extract and filter STG tables dataframe
    stg_tables_df = smx_model['stg tables']
    # Removing Nulls
    stg_tables_df = stg_tables_df.dropna(subset=['key set name', 'key domain name'])
    # Generate views directly from filtered rows using 
    scripts = []    
    print("-----stg_tables_df---------")
    print(stg_tables_df)
    for _, row in stg_tables_df.iterrows():
        print (row['bkey filter'])
        view_script = Queries.bkey_views(
            row['key set name'],
            row['key domain name'], 
            row['table name stg'],
            row['natural key'],
            row['column name stg'],
            row['bkey filter'],
            env 
        )
        scripts.append(view_script)
        
    return scripts

def bkey_views(key_set_name ,Domain_Name ,table_name_STG,natural_key,Column_Name_STG , BKEY_filter, environment):
    process_name="BK_"+key_set_name+"_"+Domain_Name+"_"+table_name_STG+"_"+Column_Name_STG
    filter_condition =""
    if not math.isnan(BKEY_filter) :
        print(BKEY_filter)
        filter_condition = f"AND {BKEY_filter}"
    return f"""
    REPLACE VIEW G{environment}1V_INP.{process_name} AS LOCK ROW FOR ACCESS 
    SELECT TRIM({natural_key}) AS SOURCE_KEY
    FROM G{environment}1V_STG.{table_name_STG}
    WHERE {natural_key} IS NOT NULL AND {natural_key} <> '' {filter_condition}
    GROUP BY 1;
    """

#---------------------------------------------------------
def insert_bmap_values(smx_model, env):
    
    # Get BMAP values dataframe from smx_model
    BMAP_values_df = smx_model['bmap values']
    # Generate views using Queries.Insert_into_BMAP_Standard_Map_table method
    scripts = []
    for _, row in BMAP_values_df.iterrows():
        sql_script = Queries.Insert_into_BMAP_Standard_Map_table(
            row['source code'],
            row['code domain id'],
            row['code set id'], 
            row['edw code'],
            row['description'],
            env
        )
        scripts.append(sql_script)        
    return scripts

def Insert_into_BMAP_Standard_Map_table(Source_Code, Domain_Id, Code_Set_Id, EDW_Code, Description , environment):
    return f"""INSERT INTO G{environment}1T_UTLFW.BMAP_Standard_Map (Source_Code, Domain_Id, Code_Set_Id, EDW_Code, Description)
    VALUES ({Source_Code}, {Domain_Id}, {Code_Set_Id}, {EDW_Code}, {Description});"""
#---------------------------------------------------------
"""
Table Name  in  CORE tables  === code set name  in  BMAP values 
--------------
relation : column name in core table  && code set name in BMAP/BMAP values
هنجيب كله من الكوووووور
والكود ست نيم من البي ماب
# Note : We use Column Name STG instead of Column Name Source   
         because Column name source may have names == reserved words so we change them in Column Name STG
"""
def create_LKP_views (smx_model, env):
    # Get BMAP values dataframe from smx_model
    BMAP_values_df = smx_model['bmap values']
    core_tables_df = smx_model['core tables'] 
    # BMAP_values_df = BMAP_values_df.rename(columns={'code set name': 'table name'})
    # Filter core tables to only include those with matching code set names == table name
    filtered_core_tables = BMAP_values_df.merge(core_tables_df, how='inner', left_on="code set name", right_on="table name")
    filtered_core_tables = (
        filtered_core_tables.groupby('code set name')
        .agg({
            'code set id': 'first',  # assuming one ID per code set
            'column name': lambda x: list(x.unique())  # unique columns per code set
        })
        .reset_index()
    )
    scripts = []
    for _, row in filtered_core_tables.iterrows():
        Desc = ''
        Code = ''
        # Iterate through the list of column names
        for col_name in row['column name']:
            if col_name.lower().endswith("desc"):
                Desc = col_name
            else:
                Code = col_name
        Code_Set_Id = row['code set id']
        table_name = row['code set name']

        sql_script = f""" REPLACE VIEW G{env}1V_CORE.{table_name} AS LOCK ROW FOR ACCESS
                SELECT DISTINCT
                EDW_Code AS {Code},
                Description AS {Desc}
                FROM G{env}1T_UTLFW.BMAP_Standard_Map
                WHERE Code_Set_Id = {Code_Set_Id};
            """
        scripts.append(sql_script)
    return scripts
#---------------------------------------------------------

def create_stg_table_and_view(smx_model, environment):
    stg_df = smx_model['stg tables']
    # Group by each STG table name
    sql_scripts = []
    for table_name_stg, group in stg_df.groupby('table name stg'):
        columns = []
        primary_key = None

        for _, row in group.iterrows():
            col_name = row['column name stg']
            col_type = row['stg data type']
            is_pk = row['pk'].lower()
            # - surrogate key && BM  columns are excluded ( any column start with SK_ or BM_ )
            if col_name.upper().startswith('SK_') or col_name.upper().startswith('BM_'):
                continue
            columns.append(f"{col_name} {col_type}")

            if is_pk == 'y':
                primary_key = col_name

        # Join all columns as string
        column_definitions = ",\n        ".join(columns)

        # Add the CREATE TABLE statement
        create_stmt = f"""
        CREATE MULTISET TABLE G{environment}1T_STG.{table_name_stg},
            FALLBACK,
            NO BEFORE JOURNAL, 
            NO AFTER JOURNAL,
            CHECKSUM = DEFAULT,
            DEFAULT MERGEBLOCKRATIO,
            MAP = TD_MAP1
        (
            {column_definitions}
            Start_Ts TIMESTAMP(6) WITH TIME ZONE,
            End_Ts TIMESTAMP(6) WITH TIME ZONE,
            Start_Date DATE,
            End_Date DATE,
            Record_Deleted_Flag BYTEINT,
            Ctl_Id SMALLINT COMPRESS 997,
            File_Id SMALLINT COMPRESS 997,
            Process_Name VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
            Process_Id INTEGER,
            Update_Process_Name VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
            Update_Process_Id INTEGER
        )
        UNIQUE PRIMARY INDEX ({primary_key});

        REPLACE VIEW G{environment}1V_STG.{table_name_stg} AS LOCK ROW FOR ACCESS SELECT * FROM G{environment}1T_STG.{table_name_stg};
    """
    sql_scripts.append(create_stmt.strip())

    return "\n\n".join(sql_scripts)
# ------------------------------------------------------------------------------------------------------
"""
- we will get  table name , Columns and its Data types from (stg table) : Table Name STG &&  Column Name STG   &&    STG Data type    columns
- decide column is Primary key or not from (stg table) :  PK    column
- surrogate key && BM  columns are excluded ( any column start with SK_ or BM_ )
"""
def create_SCRI_table(smx_model, environment):
    stg_df = smx_model['stg tables'][['table name stg', 'column name stg', 'stg data type', 'pk']].dropna()
    
    sql_scripts = []

    # Group by each STG table name
    for table_name_stg, group in stg_df.groupby('table name stg'):
        columns = []
        primary_key = None
        
        for _, row in group.iterrows():
            col_name = row['column name stg']
            col_type = row['stg data type']
            is_pk = row['pk'].lower()
            columns.append(f"{col_name} {col_type}")
         
            if is_pk == 'y':
                primary_key = col_name

        # Join all columns as string
        column_definitions = ",\n        ".join(columns)

        # Add the CREATE TABLE statement
        create_stmt = f"""
        CREATE MULTISET TABLE G{environment}1T_SRCI.{table_name_stg}
        (
                {column_definitions},
                Start_Ts TIMESTAMP(6) WITH TIME ZONE,
                End_Ts TIMESTAMP(6) WITH TIME ZONE,
                Start_Date DATE FORMAT 'YYYY-MM-DD',
                End_Date DATE FORMAT 'YYYY-MM-DD',
                Record_Deleted_Flag BYTEINT,
                Ctl_Id SMALLINT COMPRESS(997),
                File_Id SMALLINT COMPRESS(997),
                Process_Name VARCHAR(128),
                Process_Id INTEGER,
                Update_Process_Name VARCHAR(128),
                Update_Process_Id INTEGER
        ) PRIMARY INDEX({primary_key});
        """
        sql_scripts.append(create_stmt.strip())

    return "\n\n".join(sql_scripts)

# --------------------------------------------------------------------------------------------
# - Code set name & key domain name & natural key  non of them can be null value 
def create_SCRI_view(smx_model,environment):
    table_name_stg = smx_model["stg tables"]["table name stg"].dropna().unique().tolist()
    sql_scripts = []
    for table in table_name_stg:
        create_stmt = f"""
        REPLACE VIEW G{environment}1V_SRCI.{table} AS LOCK ROW FOR ACCESS SELECT * FROM G{environment}1T_SRCI.{table};
        """
        sql_scripts.append(create_stmt.strip())

    return "\n\n".join(sql_scripts)

def create_SCRI_input_view(smx_model,environment):
    stg_df = smx_model["stg tables"]
    bkey_df = smx_model["bkey"]
    filterd_bkey_df = bkey_df.merge(stg_df,on=["key set name","key domain name"],how='inner')
    filterd_bkey_df = filterd_bkey_df.drop_duplicates(subset=['key set name', 'key domain name'])[
    ['key set name', 'key domain name', 'key set id', 'key domain id', 'physical table']]

    bmap_df = smx_model["bmap"]
    filterd_bmap_df = bmap_df.merge(stg_df,on=["code set name","code domain name"],how='inner')
    filterd_bmap_df =filterd_bmap_df.drop_duplicates(subset=['code set name', 'code domain name'])[
        ['code set name', 'code domain name', 'code set id', 'code domain id']
    ]

    sql_scripts = []
    # Group by each STG table name
    for table_name_stg, group in stg_df.groupby('table name stg'):
        process_name = f"TX_SRCI_{table_name_stg}" 
        BKscolumns =[]
        SK_counter = 1
        BMscolumns=[] 
        BM_counter = 1
        joins_SK_script=[]
        joins_BM_script = []

        # list of source columns is columns without SK_ & BM_ columns
        Source_columns=[]
        conditions=[]
        joins_BKs_script=[]

        # looping on every row in the current table
        for _, row in group.iterrows():
            # from STG tables we will get natural key  of the current SK
            natural_key=row['natural key']
            # From filtered_bkey_df we will get physical table & key domain id using key set name & key domain name of the current row
            matched_row = filterd_bkey_df.loc[
                (filterd_bkey_df['key set name'] == row['key set name']) & 
                (filterd_bkey_df['key domain name'] == row['key domain name'])
            ]
            if not matched_row.empty:
                physical_table = matched_row['physical table'].iloc[0]
                key_domain_id = matched_row['key domain id'].iloc[0]

            # get current column 
            col_name = row['column name stg']
            # we need to check for key set name & key domain id & natural key  not null 
            if col_name.upper().startswith('SK_'):
                if natural_key and  row["key set name"] and row["key domain name"]:
                    # --------- Left joins -------------
                    joins_BKs_script.append(f"LEFT JOIN G{environment}1V_UTLFW.{physical_table} BK{SK_counter} ON SOURCE.{natural_key} = BK{SK_counter}.SOURCE_KEY AND BK{SK_counter}.DOMAIN_ID = {key_domain_id}")
                    
                    BKscolumns.append(f"BK{SK_counter}.EDW_KEY AS {col_name}") 
                    SK_counter += 1
                    print("SK_column:    "+  col_name)
            # we need to check for code set name & code domain id & natural key  not null 
            elif col_name.upper().startswith('BM_') :
                if natural_key and  row["code set name"] and row["code domain name"]:
                        # From BMAP we will get code set id && code domain id   using code set name && code domain name    of the current BM
                        bmap_match = filterd_bmap_df.loc[
                            (filterd_bmap_df['code set name'] == row["code set name"]) &
                            (filterd_bmap_df['code domain name'] == row["code domain name"])
                        ]
                        if not bmap_match.empty:
                            code_domain_id = bmap_match['code domain id'].iloc[0]
                            code_set_id = bmap_match['code set id'].iloc[0]
                            # --------- Left joins -------------
                            joins_BM_script.append(f"LEFT JOIN G{environment}1V_UTLFW.BMAP_STANDARD_MAP BM{BM_counter} ON SOURCE.{natural_key} = BM{BM_counter}.SOURCE_CODE AND BM{BM_counter}.CODE_SET_ID = {code_set_id} AND BM{BM_counter}.DOMAIN_ID = {code_domain_id}")
                            BMscolumns.append(f"BM{BM_counter}.EDW_CODE AS {col_name}")
                            BM_counter += 1
                            print("BM_column:    "+  col_name)
            else :
                Source_columns.append(f"SOURCE.{col_name}")
                conditions.append(f"SOURCE.{col_name} IS NOT NULL")
        # end of row loop 
        source_columns_str = ",\n".join(Source_columns)
        BKscolumns_str = ",\n".join(BKscolumns)
        BMscolumns_str = ",\n".join(BMscolumns)
        joins_BKs_script_str = ",\n".join(joins_BKs_script)
        joins_BM_script_str = ",\n".join(joins_BM_script)
        conditions_str = "\n OR ".join(conditions)
        #  ------------------- create Scrpit for current table ------------------------------
        create_stmnt = f"""
        REPLACE VIEW G{environment}1V_INP.{process_name} AS LOCK ROW FOR  ACCESS 
        SELECT
        {source_columns_str},
    SOURCE.Start_Ts,
    SOURCE.End_Ts,
    SOURCE.Start_Date,
    SOURCE.End_Date,
    SOURCE.Record_Deleted_Flag,
    SOURCE.Ctl_Id, 
    SOURCE.File_Id,
    SOURCE.Process_Name,
    SOURCE.Process_Id,
    SOURCE.Update_Process_Name,
    SOURCE.Update_Process_Id,
    1 AS GCFR_DELTA_ACTION_CODE, 
        {BKscolumns_str}           
        {BMscolumns_str}
        FROM G{environment}1V_STG.{table_name_stg} SOURCE
        {joins_BKs_script_str}
        {joins_BM_script_str}
        WHERE {conditions_str}
        """  
        sql_scripts.append(create_stmnt.strip())
    return "\n\n".join(sql_scripts)


