import pandas as pd
import streamlit as st
import io  

def load_sheet(file_content, sheet_name):
    
    df = pd.read_excel(io.BytesIO(file_content), sheet_name , na_values=[], keep_default_na=False) 
    df.columns = df.columns.str.lower()
    return df


def get_excel_sheet_names(file_content):
    return pd.ExcelFile(io.BytesIO(file_content)).sheet_names

            
def flatten_list(nested_list):
    flattened = []
    for item in nested_list:
        if isinstance(item, list):
            flattened.extend(flatten_list(item))  # recursive call for nested lists
        elif isinstance(item, str):
            flattened.append(item)  # strings are treated as single items, not iterables
        else:
            # Handle other types (int, float, etc.) by converting to string
            flattened.append(str(item))
    return flattened


def add_sql_to_dictionary(script_dict, env , stmnt) :
    script2 = ''' '''  # Initialize with triple quotes for multiline string
    for table_name, values in script_dict.items():
        stmnt=stmnt.format(table_name=table_name, env=env)
        if isinstance(values, list):
            script2 += stmnt
            # Then append all values
            for value in values:
                script2 += value + '\n'
        else:
            script2 += stmnt
            # Then append the single value
            script2 += values + '\n'
    return script2  # Return the combined script

