import streamlit as st
from util import df_utlis

tab_options = {
    "bkey": ["REG_BKEY", "REG_BKEY_DOMAIN", "REG_BKEY_PROCESS", "BKEY_CALL", "bkey_views"],
    "stream": ["STREAM"],
    "bmap": ["REG_BMAP", "REG_BMAP_DOMAIN", "Insert BMAP values", "Create LKP views"],
    "stg tables": ["create_stg_table_and_view", "create_SCRI_table", "create_SCRI_view", "create_SCRI_input_view","EXEC_SRCI"],
    "core tables": ["create_core_table", "create_core_table_view", "create_core_input_view","CORE_KEY_COL_REG"]
}
def get_action_options(tab_name):
    return tab_options[tab_name]

def get_key_type_options():
    return tab_options.keys()
    
def get_stg_table_options (stg_df_sheet, selected_key_set):
    try:
        stg_df = stg_df_sheet.copy()
        # Filter STG tables based on selected_key_set
        if selected_key_set != "All" :
            stg_df = df_utlis.filter_by_column_value(stg_df, 'key set name', selected_key_set)
            unique_tables = stg_df['table name source'].dropna().unique().tolist()
            return unique_tables, stg_df
        else:
            unique_tables = stg_df['table name source'].dropna().unique().tolist()
            return unique_tables, stg_df

    except Exception as e:
        st.error(f"Error loading/processing STG tables: {str(e)}")
        return None, None








