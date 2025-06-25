import pandas as pd
import streamlit as st
import io  

def load_excel_file(file_content):
    return pd.read_excel(io.BytesIO(file_content))
def load_sheet(file_content, sheet_name):
    return pd.read_excel(io.BytesIO(file_content), sheet_name)

def get_excel_sheet_names(file_content):
    return pd.ExcelFile(io.BytesIO(file_content)).sheet_names


def df_groupBy(df, col) :
    d = (df.groupby([col])[df.columns]
       .apply(lambda x: x.to_dict('records'))
       .reset_index(name='attr'))
    df_returned  =pd.DataFrame(d)
    return df_returned

def filter_by_column_value(df, column_name, values_to_filter):
    if column_name not in df.columns:
        return df

    if values_to_filter is None or (isinstance(values_to_filter, list) and not values_to_filter):
        return df

    # Ensure values_to_filter is a list for consistent processing
    if not isinstance(values_to_filter, list):
        values_list = [values_to_filter]
    else:
        values_list = values_to_filter

    return df[df[column_name].isin(values_list)]
            
# Get unique values from a dataframe column, removing NaN values.
def get_unique_values(df, column_name):
    if column_name in df.columns:
        return df[column_name].dropna().unique().tolist()
    return []



# Clean system data by removing rows with empty aliases and invalid values.
def clean_system_data(system_df, alias_column='Source System Alias'):
    if alias_column in system_df.columns:
        # Remove rows with empty aliases
        cleaned_df = system_df.dropna(subset=[alias_column])
        # Remove rows with whitespace-only aliases
        return cleaned_df[cleaned_df[alias_column].str.strip().astype(bool)]
    return system_df

