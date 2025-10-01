import math
from typing import Counter
from pandas.io.formats.style import Subset
import streamlit as st
from sqlalchemy import column, desc

import pandas as pd
# --------------------------------------------------------  Actions   -------------------------------------------------------------- 
def generate_bkey_views(smx_model, env):
    # Extract and filter STG tables dataframe
    stg_tables_df = smx_model['stg tables']  # this DF has no nulls in key set name and key domain name

    # Generate views directly from filtered rows using 
    scripts = []    
    for _, row in stg_tables_df.iterrows():
        key_set_name = row['key set name']
        Domain_Name = row['key domain name']
        table_name_STG = row['table name stg']
        natural_key = row['natural key']
        Column_Name_STG = row['column name stg']
        BKEY_filter = row['bkey filter']
        BKEY_join = row['bkey join']

        environment = env 
        print("filter",BKEY_filter)
        process_name="BK_"+key_set_name+"_"+Domain_Name+"_"+table_name_STG+"_"+Column_Name_STG
        filter_condition =""
        if len(BKEY_filter.strip()) > 0 :
            print("filter",BKEY_filter)
            filter_condition = f"AND {BKEY_filter}"
        if len(BKEY_join.strip()) > 0 :
            BKEY_join = f"\n {BKEY_join}"

        view_script =f"""
        REPLACE VIEW G{environment}1V_INP.{process_name} AS LOCK ROW FOR ACCESS 
        SELECT TRIM({natural_key}) AS SOURCE_KEY
        FROM G{environment}1V_STG.{table_name_STG} A {BKEY_join}
        WHERE A.{natural_key} IS NOT NULL AND A.{natural_key} <> '' {filter_condition}
        GROUP BY 1;
        """
        print(view_script)
        scripts.append(view_script)

    return scripts

#---------------------------------------------------------
def insert_bmap_values(smx_model, env):
    # Get BMAP values dataframe from smx_model
    BMAP_values_df = smx_model['bmap values']

    # Generate views 
    scripts = []
    for _, row in BMAP_values_df.iterrows():
        code_set_name = row['code set name']
        sql_script = f"""INSERT INTO G{env}1T_UTLFW.BMAP_Standard_Map (Source_Code, Domain_Id, Code_Set_Id, EDW_Code, Description, Start_Date, End_Date,Record_Deleted_Flag,Ctl_Id , PROCESS_NAME, PROCESS_ID, UPDATE_PROCESS_NAME, UPDATE_PROCESS_ID)
    VALUES ('{row['source code']}', {row['code domain id']}, {row['code set id']}, {row['edw code']}, '{row['description']}',CURRENT_DATE, DATE '9999-09-09',0,0,'BM_{code_set_name}',0,NULL,NULL);"""
        scripts.append(sql_script)        
    return scripts

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
            mandatory = row['mandatory']
            # - surrogate key && BM  columns are excluded ( any column start with SK_ or BM_ )
            if col_name.upper().startswith('SK_') or col_name.upper().startswith('BM_'):
                continue
            if mandatory.strip()!='':   
                is_mandatory = str(mandatory).lower()
                if is_mandatory == 'y':
                    columns.append(f"{col_name} {col_type} NOT NULL")
            else:
                columns.append(f"{col_name} {col_type}")


            if is_pk == 'y':
                primary_key = col_name

        # Join all columns as string
        column_definitions = ",\n        ".join(columns)

        # Add the CREATE TABLE statement
        create_stmt = f"""
        CREATE MULTISET TABLE G{environment}1T_STG.{table_name_stg}
        (
        {column_definitions},
        START_TS TIMESTAMP(6) WITH TIME ZONE,
        END_TS TIMESTAMP(6) WITH TIME ZONE,
        START_DATE DATE,
        END_DATE DATE,
        RECORD_DELETED_FLAG BYTEINT,
        CTL_ID SMALLINT COMPRESS 997,
        FILE_ID SMALLINT COMPRESS 997,
        PROCESS_NAME VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
        PROCESS_ID INTEGER,
        UPDATE_PROCESS_NAME VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
        Update_Process_Id INTEGER
        )
        PRIMARY INDEX ({primary_key});

        REPLACE VIEW G{environment}1V_STG.{table_name_stg} AS LOCK ROW FOR ACCESS SELECT * FROM G{environment}1T_STG.{table_name_stg};
    """
        sql_scripts.append(create_stmt.strip())
    print(sql_scripts)

    return "\n\n".join(sql_scripts)
# ------------------------------------------------------------------------------------------------------
"""
- we will get  table name , Columns and its Data types from (stg table) : Table Name STG &&  Column Name STG   &&    STG Data type    columns
- decide column is Primary key or not from (stg table) :  PK    column
- surrogate key && BM  columns are excluded ( any column start with SK_ or BM_ )
"""
def create_SCRI_table(smx_model, environment):
    stg_df = smx_model['stg tables']
    sql_scripts = []

    stg_df = stg_df.dropna(subset=['table name stg', 'column name stg', 'stg data type'])
    # print(stg_df.columns)

    # Group by each STG table name
    for table_name_stg, group in stg_df.groupby('table name stg'):
        columns = []
        primary_key = None
        
        for _, row in group.iterrows():
            col_name = row['column name stg']
            col_type = row['stg data type']
            is_pk = row['pk'].lower()
            mandatory = row['mandatory']
            if mandatory.strip() !='' :
                is_mandatory = str(mandatory).lower()
                if is_mandatory == 'y':
                    primary_key = col_name
                    columns.append(f"{col_name} {col_type} NOT NULL")
            else:
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
            Process_Name VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
            Process_Id INTEGER,
            Update_Process_Name VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
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

def create_SCRI_input_view(smx_model, environment):
    stg_df = smx_model["stg tables"]
    bkey_df = smx_model["bkey"]

    filterd_bkey_df = bkey_df.merge(stg_df, on=["key set name", "key domain name"], how='inner')
    filterd_bkey_df = filterd_bkey_df.drop_duplicates(subset=['key set name', 'key domain name'])[
        ['key set name', 'key domain name', 'key set id', 'key domain id', 'physical table'] 
    ]

    bmap_df = smx_model["bmap"]
    filterd_bmap_df = bmap_df.merge(stg_df, on=["code set name", "code domain name"], how='inner')
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

        # Build a mapping from STG column names to “SOURCE.<col>” aliases
        col_names = group['column name stg'].tolist()
        natural_keys = group['natural key'].str.strip().tolist()
        # Replace every natural-key placeholder with its SOURCE-qualified column
        for col in col_names:
            if col in natural_keys:  # natural key exists as a column
                # natural_keys[natural_keys.index(col)] = f"SOURCE.{col}"

                group['natural key'].replace(col, f"SOURCE.{col}", inplace=True)
                
                # print(group[group['natural key']==col])
            else:
                target = f" {col} "
                for nk in natural_keys:
                    if target in nk:
                        temp = nk.replace(target, f" SOURCE.{col} ")
                        group['natural key'].replace(nk, temp, inplace=True)


        # natural_keys list is now ready to be used in JOIN conditions
        for _, row in group.iterrows():
            # from STG tables we will get natural key  of the current SK
            natural_key=row['natural key'].strip()
            # From filtered_bkey_df we will get physical table & key domain id using key set name & key domain name of the current row
            matched_row = filterd_bkey_df.loc[
                (filterd_bkey_df['key set name'] == row['key set name']) & 
                (filterd_bkey_df['key domain name'] == row['key domain name'])
            ]
            if not matched_row.empty:
                physical_table = matched_row['physical table'].iloc[0]  # Changed from 'physical table'
                key_domain_id = matched_row['key domain id'].iloc[0]

            # get current column 
            col_name = row['column name stg']
            # we need to check for key set name & key domain id & natural key  not null 
            if col_name.upper().startswith('SK_'):
                if natural_key!='' and  row["key set name"] and row["key domain name"]:
                    # --------- Left joins -------------
                    joins_BKs_script.append(f"LEFT JOIN G{environment}1V_UTLFW.{physical_table} BK{SK_counter} ON {natural_key} = BK{SK_counter}.SOURCE_KEY AND BK{SK_counter}.DOMAIN_ID = {key_domain_id}")
                    
                    BKscolumns.append(f"BK{SK_counter}.EDW_KEY AS {col_name}") 
                    SK_counter += 1
                    print("SK_column:    "+  col_name)
            # we need to check for code set name & code domain id & natural key  not null 
            elif col_name.upper().startswith('BM_') :
                if natural_key!='' and  row["code set name"] and row["code domain name"]:
                        # From BMAP we will get code set id && code domain id   using code set name && code domain name    of the current BM
                        bmap_match = filterd_bmap_df.loc[
                            (filterd_bmap_df['code set name'] == row["code set name"]) &
                            (filterd_bmap_df['code domain name'] == row["code domain name"])
                        ]
                        if not bmap_match.empty:
                            code_domain_id = bmap_match['code domain id'].iloc[0]
                            code_set_id = bmap_match['code set id'].iloc[0]
                            # --------- Left joins -------------
                            joins_BM_script.append(f"LEFT JOIN G{environment}1V_UTLFW.BMAP_STANDARD_MAP BM{BM_counter} ON {natural_key} = BM{BM_counter}.SOURCE_CODE AND BM{BM_counter}.CODE_SET_ID = {code_set_id} AND BM{BM_counter}.DOMAIN_ID = {code_domain_id}")
                            BMscolumns.append(f"BM{BM_counter}.EDW_CODE AS {col_name}")
                            BM_counter += 1
                            print("BM_column:    "+  col_name)
            else :
                Source_columns.append(f"SOURCE.{col_name}")
                conditions.append(f"SOURCE.{col_name} IS NOT NULL")
        # end of row loop 
        # i have added empty string here to the end of  Source_columns list to always add comme and new line at the end of SK columns
        source_columns_str = ",\n".join(Source_columns+ [""])
        BKscolumns_str = ",\n".join(BKscolumns)+ ",\n" if len(BMscolumns)>0 else ",\n".join(BKscolumns)
        BMscolumns_str = ",\n".join(BMscolumns)
        
        joins_BKs_script_str = "\n".join(joins_BKs_script)
        joins_BM_script_str = "\n".join(joins_BM_script)
        conditions_str = "\n OR ".join(conditions)
        #  ------------------- create Scrpit for current table ------------------------------
        create_stmnt = f"""REPLACE VIEW G{environment}1V_INP.{process_name} AS LOCK ROW FOR ACCESS 
SELECT
{source_columns_str}
SOURCE.START_TS,
SOURCE.END_TS,
SOURCE.START_DATE,
SOURCE.END_DATE,
SOURCE.RECORD_DELETED_FLAG,
SOURCE.CTL_ID,
SOURCE.FILE_ID,
SOURCE.PROCESS_NAME,
SOURCE.PROCESS_ID,
SOURCE.UPDATE_PROCESS_NAME,
SOURCE.UPDATE_PROCESS_ID,
1 AS GCFR_DELTA_ACTION_CODE,
{BKscolumns_str}           
{BMscolumns_str}
FROM G{environment}1V_STG.{table_name_stg} SOURCE
{joins_BKs_script_str}
{joins_BM_script_str}
WHERE {conditions_str};
        """  
        sql_scripts.append(create_stmnt.strip())
    return "\n\n".join(sql_scripts)


def create_core_table(smx_model, environment):
    # Include 'table name' and 'pk' columns in the selection
    core_tables_df = smx_model["core tables"]
    core_tables_df = core_tables_df.dropna(subset=['column name','data type'])

    sql_scripts = []
    
    for table_name, group in core_tables_df.groupby('table name'):
        columns = []
        primary_key = []

        for _, row in group.iterrows():
            col_name = row['column name']
            col_type = row['data type']
            mandatory = row['mandatory']

            # ALL mandatory OR PK columns must be not null  (unique)
            if mandatory.strip() !='' :
                is_mandatory = str(mandatory).lower()
                if is_mandatory == 'y':
                    columns.append(f"{col_name} {col_type} NOT NULL")
            else:
                columns.append(f"{col_name} {col_type}")

            # Handle NaN values in pk column
            pk_value = row['pk']
            if pd.notna(pk_value):
                is_pk = str(pk_value).lower()
                if is_pk == 'y':
                    primary_key.append(col_name)
        # Join all columns as string
        column_definitions = ",\n        ".join(columns)

        # Fixed the double commas in the CREATE statement
        create_stmnt = f"""
        CREATE MULTISET TABLE G{environment}1T_CORE.{table_name}
    (
        {column_definitions},
        START_TS TIMESTAMP(6) WITH TIME ZONE,
        END_TS TIMESTAMP(6) WITH TIME ZONE,
        START_DATE DATE FORMAT 'YYYY-MM-DD',
        END_DATE DATE FORMAT 'YYYY-MM-DD',
        RECORD_DELETED_FLAG BYTEINT,
        CTL_ID SMALLINT COMPRESS 997,
        FILE_ID SMALLINT COMPRESS 997,
        PROCESS_NAME VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
        PROCESS_ID INTEGER,
        UPDATE_PROCESS_NAME VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
        UPDATE_PROCESS_ID INTEGER
    )PRIMARY INDEX ({",".join(primary_key)});
    """
        sql_scripts.append(create_stmnt.strip())
    
    return "\n\n".join(sql_scripts)
def create_core_table_view(smx_model, environment):
    core_tables_DF = smx_model['core tables'][['table name']].drop_duplicates()
    
    sql_scripts = []
    for _,row in core_tables_DF.iterrows():
        table_name = row['table name']
        create_stmnt =f"""
        REPLACE VIEW G{environment}1V_CORE.{table_name} AS LOCK ROW FOR ACCESS SELECT * FROM G{environment}1T_CORE.{table_name};
        """
        sql_scripts.append(create_stmnt.strip())
    return "\n\n".join(sql_scripts)




# Data will be passed filtered from UI  
def create_core_input_view(smx_model,environment):  
    # table_mapping_df and column_mapping_df  is already filtered from interface
    table_mapping_df = smx_model['table mapping']
    # Exclude lookups (tables without subject area)
    core_tables_df_original = smx_model['core tables'].dropna(subset=['subject area','table name'])

    column_mapping_df_original = smx_model['column mapping']
    sql_scripts =[]
    
    # looping on mapping names 
    for _,row in table_mapping_df.iterrows():
        target_table_name=row['target table name']
        mapping_name = row['mapping name']
        main_source = f"{row['main source']}"
        cast_columns_list=[]
        
        # filter column_mapping_df by current mapping name only
        column_mapping_df = column_mapping_df_original[column_mapping_df_original['mapping name'] == mapping_name]
        # To avoid duplicate columns in "ALL" mapping names option   eg:EMPLOYMENT_START_DTTM exists in 2 tables and make error when checking for pk
        core_tables_df = core_tables_df_original[core_tables_df_original['table name'] == target_table_name]
        process_name = f"TX_{mapping_name}"
        historization_algorithm= row['historization algorithm'].upper()
        mapped_to_table = row['mapped to']

        joins = f"{row['join']}" if row['join'].strip() !='' else '' 
        Filter_criterion = f"WHERE {row['filter criterion']}" if row['filter criterion'].strip() !='' else ''

        # Create a lookup dictionary once for core tables primary keys (more efficient for multiple lookups)
        pk_lookup = dict(zip(core_tables_df['column name'], core_tables_df['pk']))

        counter=0
        Counter_list=[]
        found_aggregat = False    
        join_on_pk_columnn=[]
        PK_column = ''
        # ----------------------------------------------  Column Mapping loop ----------------------------------------------
        for _, row in column_mapping_df.iterrows():

            transformation_type = row['transformation type'].upper()  
            mapped_to_column = row['mapped to column']  # column name in the source table 
            
            mapped_to_table = row['mapped to table']    # table name in the source table 
            transformation_rule = str(row['transformation rule']).upper() 
            column_name = row['column name'] # column name in the target tabel

                
            # adding Alias to columns in the transformation rule 
            if transformation_rule!='' and transformation_type.upper()!= "CONST" and mapped_to_table !='':
                transformation_rule = transformation_rule.replace(mapped_to_column, mapped_to_table + '.' + mapped_to_column) 
            # adding Alias to the source columns  
            mapped_to_column = mapped_to_table + '.' + mapped_to_column   if mapped_to_table.strip() !='' else mapped_to_column

            # get the data type of column name in column mapping sheet from core table  sheet
            data_type = core_tables_df[(core_tables_df['column name'] == column_name) ]['data type'].iloc[0]    
                
            #TODO : not working in first and second
            if 'timestamp' in data_type.lower() or 'date' in data_type.lower() :
                data_type = data_type+" FORMAT 'YYYYMMDD'"
        
            if historization_algorithm != 'INSERT' :    
                pk = pk_lookup.get(column_name)
                if pk and str(pk).lower() == 'y' :
                    if str(column_name).lower().endswith('_id'):
                        PK_column = column_name
                    if mapped_to_column:
                        join_on_pk_columnn.append(f"{target_table_name}.{column_name} = {mapped_to_column}")


            counter+=1 
            Counter_list.append(counter)
            if(transformation_type == 'COPY'): 
                cast_columns_list.append(f"CAST({mapped_to_column} AS {data_type}) AS {column_name}")

            elif(transformation_type == 'SQL'):

                cast_columns_list.append(f"CAST({transformation_rule} AS {data_type}) AS {column_name}")
                # in case we have aggregate function in transformation rule, we should make group by all columns numbers except the column with aggregate
                # TODO : also make sure this aggregatesare all we search for 
                aggregations =[ 'AVG(','AVG (','MIN(','MIN (','MAX(','MAX (','COUNT(','COUNT (', 'SUM(']
                matches = [agg for agg in aggregations if agg.upper() in transformation_rule.upper()]

                if matches :
                    found_aggregat=True
                    Counter_list.pop()
            
            elif(transformation_type=='CONST'):
                if transformation_rule != 'NULL' and not transformation_rule.isnumeric():
                    transformation_rule = f"'{transformation_rule}'"
                cast_columns_list.append(f"CAST({transformation_rule} AS {data_type}) AS {column_name}")
        
        temp = f"LEFT JOIN G{environment}1V_CORE.{target_table_name} AS {target_table_name} ON "
        pk_joins_stmnt = temp+" AND \n".join(join_on_pk_columnn) if join_on_pk_columnn else ''


        cast_columns_stmnt = ",\n  ".join(cast_columns_list)
        groub_by_stmnt=""
        # add number of Technical columns (11 column)
        Counter_list =  Counter_list + list(range(counter+1, counter+9))

        if found_aggregat :
            groub_by_stmnt = f"GROUP BY {', '.join(str(x) for x in Counter_list)}" 
        if str(historization_algorithm) == "INSERT":
            select_stmnt = "1 AS GCFR_DELTA_ACTION_CODE"
        # in case of UPSERT  &  HISTORY
        else:
            select_stmnt=f"""(SELECT BUSINESS_DATE FROM G{environment}1V_GCFR.GCFR_PROCESS_ID WHERE PROCESS_NAME='{process_name}') AS Start_Date,
                DATE '9999-09-09' AS End_Date,
                CASE WHEN {target_table_name}.{PK_column} IS NULL THEN 0 ELSE 1 END AS Record_Deleted_Flag,
                (SELECT Ctl_Id FROM G{environment}1V_GCFR.GCFR_Process WHERE PROCESS_NAME='{process_name}') AS Ctl_Id,
                '{process_name}' AS Process_Name,
                (SELECT Process_ID FROM G{environment}1V_GCFR.GCFR_PROCESS_ID WHERE PROCESS_NAME='{process_name}') AS Process_ID,
                NULL AS update_Process_Name,
                NULL AS update_Process_Id """ 

        create_stmnt =  f"""
        REPLACE VIEW G{environment}1V_INP.TX_{mapping_name} AS LOCK ROW FOR ACCESS
        SELECT DISTINCT
        /* Target Table: 	{target_table_name} */
        /* Table Mapping:	{mapping_name} */
        /*Source Table:		{main_source} */
        {cast_columns_stmnt},
        {select_stmnt}
        FROM G{environment}1V_SRCI.{main_source} AS {main_source}
        {joins}
        {pk_joins_stmnt}
        {Filter_criterion}
        {groub_by_stmnt};
    """
# TODO : we might have several tables to join with 
        sql_scripts.append(create_stmnt.strip())
    return "\n\n".join(sql_scripts)


