import streamlit as st
from streamlit_ace import st_ace

import pandas as pd
import io
import os # Added import
from generating_scripts import load_script_model, main
from util import df_utlis
from util import tab_operations
from util import Queries
from datetime import datetime
from util.auth import check_authentication

# Authentication check - must be first command
authenticator = check_authentication()


# Move file uploader to sidebar
with st.sidebar:
    # Upload file and store in session state
    uploaded_file = st.file_uploader("Upload the SMX file", type=['xlsx'])
    if uploaded_file is not None:
        st.session_state['uploaded_file'] = uploaded_file
    elif 'uploaded_file' in st.session_state:
        uploaded_file = st.session_state['uploaded_file']
    
    authenticator.logout('Logout', 'sidebar')
    
# Process the file when uploaded
if uploaded_file is not None:
    file_content = uploaded_file.getvalue()
    sheets_names = df_utlis.get_excel_sheet_names(file_content)
    bkey_sheet = df_utlis.load_sheet(file_content, "BKEY")
    stg_tables_sheet = df_utlis.load_sheet(file_content, "STG tables")
    Stream_sheet = df_utlis.load_sheet(file_content, "Stream")
    # bmap_values_sheet = filtered_bmap_values_df
    bmap_sheet = df_utlis.load_sheet(file_content, "BMAP")
    core_tables_sheet = df_utlis.load_sheet(file_content, "CORE tables")
    
    excel_file_name = uploaded_file.name.split('.')[0] # Get Excel file name without file extension for exporting query to file
    col1, col2 , col3 = st.columns(3)    

    with col1:
        selected_environment = st.selectbox(
            "Select Environment :",
            options=["TST", "DEV", "PROD"],
            index=0,
            key="select_environment",
        )
    with col2:
        selected_tab = st.selectbox(
            "Select key area :",
            options=sheets_names,
            index=0,
            key="tab_selector",
        )
    tab_name = selected_tab
            
    # Action / Query
    with col3:
        selected_action = st.selectbox(
            f"Select Key type:",
            options=tab_operations.get_action_options(tab_name),
            key="action" 
        )
        
    col1, col2 , col3 = st.columns(3)    
    # Key Set Name
    with col1:
        Frequency = st.selectbox(
            "Select Frequency",
            options =["Daily", "weekly" , "Monthly"],
            key="Frequency" 
        )
       
    # Data Type (INT or BIG INT)
    with col2:
        data_type = st.selectbox(
            "Select INT Type:",
            options =["int", "BIG int"],
            key="data_type",
            # disabled=(tab_name != "BKEY")  
        )

    with col3:
        key_set_names = bkey_sheet['Key Set Name'].dropna().unique().tolist()
        selected_key_set = st.selectbox( 
        "Select Key Set Name:", 
        options = key_set_names,
        key="key_set_name",
        disabled=(tab_name != "BKEY")  # Disable when BKEY tab is selected
    )
    filtered_key_set_names_DF = df_utlis.filter_by_column_value(bkey_sheet, 'Key Set Name', selected_key_set)
    # Filtered STG tables By Key Set Name
    tables_names , STG_tables_df = tab_operations.get_stg_table_options(stg_tables_sheet, selected_key_set)
    # key domain name Column in ( STG Tables ) which is filtered already by key set name 
    key_domain_names = df_utlis.get_unique_values(STG_tables_df, 'Key Domain Name')

    multi_col1, multi_col2 = st.columns(2)

    # Key Domain Name 
    with multi_col1:
        selected_domains = st.multiselect(
            "Select Key Domain :",
            options=key_domain_names,
            key="key_domain_name",
            disabled=(tab_name != "BKEY")  
        )
    # filtered bkey df based on key set name && key domain name
    filtered_bkey_df = df_utlis.filter_by_column_value(filtered_key_set_names_DF, 'Key Domain Name', selected_domains)
    # STG Tables 
    with multi_col2:
        selected_tables = st.multiselect(
            "Select Table :",
            options= tables_names,
            key="stg_tables",
            disabled=(tab_name == "BMAP") 
        )
        filterd_STG_tables_df = df_utlis.filter_by_column_value(STG_tables_df, 'Table Name Source', selected_tables)
    print(selected_tables)
    multi_coll1, multi_coll2 = st.columns(2)
    # Get BMAP dataframe
    BMAP_df = df_utlis.load_sheet(file_content, "BMAP values")
    # Create columns for selections
    with multi_coll1:
        # Show unique values from BMAP_df for selection
        selected_code_set_names = st.multiselect(
            "Select Code Set Name:",
            options=BMAP_df['Code Set Name'].unique(),
            key="code_set_names",
            disabled=(tab_name == "BKEY")
        )
        
    with multi_coll2:
        selected_code_domain_names = st.multiselect(
            "Select Code Domain Name:",
            options=BMAP_df['Code Domain Name'].unique(),
            key="code_domain_names",
            disabled=(tab_name == "BKEY")
        )
    filtered_bmap_values_df=[]
    # Filter and display selected rows
    filtered_bmap_values_df = BMAP_df.copy()

    if selected_code_set_names:
        filtered_bmap_values_df = filtered_bmap_values_df[filtered_bmap_values_df['Code Set Name'].isin(selected_code_set_names)]

    if selected_code_domain_names:
        filtered_bmap_values_df = filtered_bmap_values_df[filtered_bmap_values_df['Code Domain Name'].isin(selected_code_domain_names)]

#----------------------------------------   Process Selected Action  -------------------------------------------------------
    if data_type == "int":
        bigint_flag = "0"
    elif data_type == "BIG int":
        bigint_flag = "1"

    key_set_id = bkey_sheet[bkey_sheet['Key Set Name'] == selected_key_set]['Key Set ID'].iloc[0]
    Key_Domain_ID = bkey_sheet[bkey_sheet['Key Set Name'] == selected_key_set]['Key Domain ID'].iloc[0]

#---------------------------------------    Display query editor and execute query    ---------------------------------------
    Dict = {
            # "BKEY" : filtered_bkey_df,
            "BKEY":bkey_sheet,
            # "STG tables" : filterd_STG_tables_df,
            "STG tables" :stg_tables_sheet,
            "Stream" : Stream_sheet,
            "BMAP values" : filtered_bmap_values_df,
            "BMAP" : bmap_sheet,
            "CORE tables": core_tables_sheet,
        }
    # st.write(filterd_STG_tables_df)
    print("------Main---------")
    smx_model = {k.lower(): v for k, v in Dict.items()}

    # Process each DataFrame: lowercase
    for key in smx_model:
        smx_model[key].columns = [col.lower() for col in smx_model[key].columns]
   
    stg_df = smx_model["stg tables"]
    bkey_df = smx_model["bkey"]
    filterd_bkey_df = bkey_df.merge(stg_df,on=["key set name","key domain name"],how='inner')
    filterd_bkey_df = filterd_bkey_df.drop_duplicates(subset=['key set name', 'key domain name'])[
    ['key set name', 'key domain name', 'key set id', 'key domain id', 'physical table']]
    st.write(filterd_bkey_df)


    # At the top of your script logic, after the button columns
    gen_query_col, export_query_col, exec_query_col = st.columns(3) 

    # Initialize session state if not exists
    if "generated_query" not in st.session_state:
        st.session_state["generated_query"] = ""
    with gen_query_col:
        if st.button(f"Generate Query", key=f"Generate_Query_Bttn"):
            script = main(smx_model, selected_action, selected_environment, bigint_flag)
            
            # Format the script output
            if script and isinstance(script, list):
                flattened_queries = []
                for item in script:
                    if isinstance(item, list):
                        flattened_queries.extend(item)
                    else:
                        flattened_queries.append(item)
                query_for_editor = '\n'.join(flattened_queries)
            else:
                query_for_editor = str(script) if script else ""
                
            st.session_state["generated_query"] = query_for_editor
            st.rerun()  # Refresh to show the updated content

    # Display the query editor (outside the button condition)
        # Display the query editor (outside the button condition)
#     current_query_in_editor = st_ace(
#     value=st.session_state["generated_query"],
#     language='sql',
#     # theme='github',  # or 'github', 'tomorrow', etc.
#     key="query_editor_widget",
#     height=400,
#     auto_update=True,
# )

    current_query_in_editor = st.text_area(
        "Query:",
        value=st.session_state["generated_query"],
        height=250,
        key="query_editor_widget"
    )

    # Update the export and execute sections to use the widget value
    with export_query_col:
        if st.button(f"Export Query", key=f"Export_Query_Bttn"):
            if not current_query_in_editor.strip():
                st.error("No query to export. Please generate a query first.")
            else:
                query_to_export = current_query_in_editor  # Use the widget value
                
                # Create directory structure and save query
                base_path = "Exported Queries"
                env_folder_path = os.path.join(base_path, selected_environment)
                excel_folder_path = os.path.join(env_folder_path, excel_file_name)
                tab_folder_path = os.path.join(excel_folder_path, tab_name)
                
                # Get current date for folder name
                current_date = datetime.now().strftime("%Y-%m-%d")
                date_folder_path = os.path.join(tab_folder_path, current_date)
                
                # Create action folder inside date folder
                action_folder_path = os.path.join(date_folder_path, selected_action)
                
                # Create directories if they don't exist
                os.makedirs(action_folder_path, exist_ok=True)
                
                # Get full timestamp and username for file name
                full_timestamp = datetime.now().strftime("%H%M%S")
                file_name = f"{full_timestamp}__{st.session_state['username']}.sql"
                query_file_path = os.path.join(action_folder_path, file_name)
                
                # Prepare query content
                query_content = f"{query_to_export}"
                
                try:
                    with open(query_file_path, "w") as f:
                        f.write(query_content)
                    st.success(f"Query exported to: {query_file_path}")
                except Exception as e:
                    st.error(f"Error exporting query: {e}")
    
    with exec_query_col:
        if st.button(f"Execute Query", key=f"Execute_Query_Bttn"):
            if not current_query_in_editor.strip():
                st.error("No query to execute. Please generate a query first.")
            else:
                # Ensure current_query_in_editor reflects the latest from session_state before executing
                query_to_execute = current_query_in_editor
                st.success(f"Executed: {query_to_execute}")
#------------------------------------------------------------------------------------------------------------------------------
else:
    st.info("Please upload an Excel file to begin.")
