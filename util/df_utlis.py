import pandas as pd
import streamlit as st
import io  

def load_sheet(file_content, sheet_name):
    
    df = pd.read_excel(io.BytesIO(file_content), sheet_name , na_values=[], keep_default_na=False) 
    df.columns = df.columns.str.lower()
    return df


def get_excel_sheet_names(file_content):
    return pd.ExcelFile(io.BytesIO(file_content)).sheet_names

def filter_by_column_value(df, column_name, values_to_filter):
    # Ensure values_to_filter is a list for consistent processing
    if not isinstance(values_to_filter, list):
        values_list = [values_to_filter]
    else:
        values_list = values_to_filter

    return df[df[column_name].isin(values_list)]
            

    