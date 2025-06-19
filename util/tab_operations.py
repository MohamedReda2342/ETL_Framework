# Utility module for handling tab operations in the ETL Framework.

from enum import unique
import streamlit as st
import pandas as pd
from util import Queries, df_utlis 

def get_key_set_options(file_content, selected_tables):
    try:
        bkey_sheet = df_utlis.load_sheet(file_content, "BKEY")
        stg_sheet = df_utlis.load_sheet(file_content, "STG tables")
        
        if 'Key Set Name' not in bkey_sheet.columns:
            return ["Key Set Name column not found in BKEY sheet"],None
            
        # Get all unique key set names from BKEY sheet
        key_set_names = df_utlis.get_unique_values(bkey_sheet, 'Key Set Name')
        # If tables are selected, filter key set names based on STG tables
        if selected_tables:
            # Filter STG sheet based on selected tables and get unique key set names
            # stg_df = stg_sheet[stg_sheet['Table Name Source'].isin(selected_tables)]
            stg_df = df_utlis.filter_by_column_value(stg_sheet, 'Table Name Source', selected_tables)
            key_set_names = df_utlis.get_unique_values(stg_df, 'Key Set Name')
        else:
            # Return all unique key set names from BKEY sheet
            key_set_names = df_utlis.get_unique_values(bkey_sheet, 'Key Set Name')
            
        return  key_set_names,bkey_sheet
        
    except Exception as e:
        return [f"Error loading sheets: {str(e)}"]
  
def get_action_options(tab_name):
    if tab_name == "BKEY" :
        return ["REG_BKEY","REG_BKEY_DOMAIN","REG_BKEY_PROCESS","BKEY_CALL","bkey_views"]
    # elif tab_name == "System":
    #     return "register_system"
    # elif tab_name == "Stream":
    #     return "register_stream"
                
# def get_domain_name_options(file_content, selected_key_set):
#     if not selected_key_set:
#         return [], None
#     bkey_sheet = df_utlis.load_sheet(file_content, "BKEY")
#     bkey_DF = bkey_sheet.copy()
    
#     if selected_key_set != "All":
#         bkey_DF = df_utlis.filter_by_column_value(bkey_DF, 'Key Set Name', selected_key_set)
#         return df_utlis.get_unique_values(bkey_DF, 'Key Domain Name'), bkey_DF
#     else:
#         return df_utlis.get_unique_values(bkey_DF, 'Key Domain Name'), bkey_DF

def get_stg_table_options (file_content, selected_key_set):
    try:
        stg_df_sheet = df_utlis.load_sheet(file_content, "STG tables")
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




