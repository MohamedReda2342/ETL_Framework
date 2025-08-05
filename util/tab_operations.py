# Utility module for handling tab operations in the ETL Framework.

from enum import unique
import tabnanny
import streamlit as st
import pandas as pd
from util import Queries, df_utlis 

def get_action_options(tab_name):
    if tab_name == "BKEY" :
        return ["REG_BKEY","REG_BKEY_DOMAIN","REG_BKEY_PROCESS","BKEY_CALL","bkey_views"]
    elif tab_name == "Stream":
        return "STREAM"
    elif tab_name =="BMAP":
        return ["REG_BMAP" , "REG_BMAP_DOMAIN" ,"Insert BMAP values" , "Create LKP views"]
    elif tab_name =="STG tables":
        return ["create_stg_table_and_view","create_SCRI_table" ,"create_SCRI_view","create_SCRI_input_view"]
    elif tab_name =="CORE tables":
        return ["create_core_table","create_core_table_view","CORE_KEY_COL_REG","create_core_input_view"]

def get_stg_table_options (stg_df_sheet, selected_key_set):
    try:
        stg_df = stg_df_sheet.copy()
        # Filter STG tables based on selected_key_set
        if selected_key_set != "All" :
            stg_df = df_utlis.filter_by_column_value(stg_df, 'Key Set Name', selected_key_set)
            unique_tables = df_utlis.get_unique_values(stg_df, 'Table Name Source')
            return unique_tables, stg_df
        else:
            unique_tables = df_utlis.get_unique_values(stg_df, 'Table Name Source')
            return unique_tables, stg_df

    except Exception as e:
        st.error(f"Error loading/processing STG tables: {str(e)}")
        return None, None




