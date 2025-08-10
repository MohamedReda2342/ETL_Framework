import streamlit as st
from streamlit_ace import st_ace
import pandas as pd
import io
import os 
import generating_scripts
from util import df_utlis
from util import tab_operations
from util import Queries
from datetime import datetime
from util.auth import check_authentication

# Authentication check - must be first command
# authenticator = check_authentication()

# Move file uploader to sidebar
with st.sidebar:
    # Upload file and store in session state
    uploaded_file = st.file_uploader("Upload the SMX file", type=['xlsx'])
    if uploaded_file is not None:
        st.session_state['uploaded_file'] = uploaded_file
    elif 'uploaded_file' in st.session_state:
        uploaded_file = st.session_state['uploaded_file']
    
#     authenticator.logout('Logout', 'sidebar')
    
# Process the file when uploaded
if uploaded_file is not None:
    file_content = uploaded_file.getvalue()
    sheets_names = df_utlis.get_excel_sheet_names(file_content)

    bkey_sheet = df_utlis.load_sheet(file_content, "BKEY")
    stg_tables_sheet = df_utlis.load_sheet(file_content, "STG tables")
    Stream_sheet = df_utlis.load_sheet(file_content, "Stream")
    # bmap_values_sheet = filtered_bmap_values_df
    bmap_sheet = df_utlis.load_sheet(file_content, "BMAP")
    bmap_values_sheet = df_utlis.load_sheet(file_content, "BMAP Values")

    core_tables_sheet = df_utlis.load_sheet(file_content, "CORE tables")
    table_mapping_sheet = df_utlis.load_sheet(file_content, "Table mapping")
    column_mapping_sheet = df_utlis.load_sheet(file_content, "Column mapping")
    
    excel_file_name = uploaded_file.name.split('.')[0] # Get Excel file name without file extension for exporting query to file
    col1, col2 , col3 = st.columns(3)    

    with col1:
        selected_environment = st.selectbox(
            "Environment :",
            options=["TST", "DEV", "PROD"],
            index=None,  # No initial selection
            placeholder="Choose an option..."  ,
            key="select_environment",
        )
    with col2:
        selected_tab = st.selectbox(
            "key area :",
            options=tab_operations.get_key_type_options(),
            index=0,
            key="tab_selector",
        )
    tab_name = selected_tab
            
    # Action / Query
    with col3:
        selected_action = st.selectbox(
            f"Key type:",
            options=tab_operations.get_action_options(tab_name),
            key="Key type" 
        )
        
    col1, col2 , col3 = st.columns(3)    
    # Key Set Name
    with col1:
        Frequency = st.selectbox(
            "Frequency",
            options =["Daily", "weekly" , "Monthly"],
            index=None,  # No initial selection
            placeholder="Choose an option..."  ,
            key="Frequency" 
        )
       
    with col2:
        data_type = st.selectbox(
            "INT Type:",
            options =["int", "BIG int"],
            index=None,  # No initial selection
            placeholder="Choose an option..."  ,
            key="data_type",
            # disabled=(tab_name != "BKEY")  
        )

    with col3:
        # Get base options
        all_key_set_names = bkey_sheet['key set name'].replace('', pd.NA).dropna().unique().tolist()
        
        selected_key_set = st.selectbox( 
            "Key Set Name (bkey sheet):", 
            options = all_key_set_names,
            index=None,  # No initial selection
            placeholder="Choose an option..."  ,
            key="key_set_name",
            disabled=(selected_action not in
              ["STREAM","BKEY_CALL","REG_BKEY","REG_BKEY_PROCESS","REG_BKEY_DOMAIN","bkey_views","create_SCRI_input_view"])  
        )
    # Filtered BKEY sheet By Key Set Name
    filtered_key_set_names_DF = bkey_sheet[bkey_sheet['key set name'].isin([selected_key_set])] if selected_key_set else bkey_sheet
    # Filtered STG tables By Key Set Name
    tables_names , STG_tables_df = tab_operations.get_stg_table_options(stg_tables_sheet, selected_key_set)

    key_domain_names = filtered_key_set_names_DF['key domain name'].dropna().unique().tolist()
    
    multi_col1, multi_col2 = st.columns(2)
    # Key Domain Name 
    with multi_col1:
        selected_domains = st.multiselect(
            "Key Domain (bkey sheet):",
            options=key_domain_names,
            key="key_domain_name",
        disabled=(selected_action not in
          ["STREAM","BKEY_CALL","REG_BKEY","REG_BKEY_PROCESS","REG_BKEY_DOMAIN","bkey_views","create_SCRI_input_view"])  
        )
    # filtered bkey df based on key set name && key domain name
    filtered_bkey_df = filtered_key_set_names_DF[filtered_key_set_names_DF['key domain name'].isin(selected_domains)] if selected_domains else filtered_key_set_names_DF

    # STG Tables 
    with multi_col2:
        # Get all STG table options
        all_stg_tables = stg_tables_sheet['table name source'].dropna().unique().tolist()
        # Filter STG tables based on selected key set
        if selected_key_set:
            stg_table_options = stg_tables_sheet[stg_tables_sheet['key set name'] == selected_key_set]['table name source'].dropna().unique().tolist()
        else:
            stg_table_options = all_stg_tables

        selected_tables = st.multiselect(
            "STG table :",
            options=stg_table_options,
            key="stg_tables",
            disabled=(selected_action not in ["STREAM","BKEY_CALL","bkey_views","REG_BKEY_PROCESS","create_stg_table_and_view","EXEC_SRCI","create_SCRI_table",
            "create_SCRI_view","create_SCRI_input_view"])
        )

    # Filter STG tables dataframe
    filterd_STG_tables_df = stg_tables_sheet[stg_tables_sheet['table name source'].isin(selected_tables)] if selected_tables else stg_tables_sheet

    st.write(filterd_STG_tables_df)
    
    multi_coll1, multi_coll2 = st.columns(2)
    # Get BMAP dataframe
    # Create columns for selections
    with multi_coll1:
        # Show unique values from bmap_sheet for selection
        selected_code_set_names = st.multiselect(
            "Select Code Set Name:",
            options=bmap_sheet['code set name'].unique(),
            key="code_set_names",
            disabled=(selected_action not in ["REG_BMAP","REG_BMAP_DOMAIN","Insert BMAP values","Create LKP views"])
        )
        
    with multi_coll2:
        selected_code_domain_names = st.multiselect(
            "Select Code Domain Name:",
            options=bmap_sheet['code domain name'].unique(),
            key="code_domain_names",
            disabled=(selected_action not in ["REG_BMAP","REG_BMAP_DOMAIN","Insert BMAP values","Create LKP views"])
        )
    filtered_bmap_values_df=[]
    # Filter and display selected rows
    filtered_bmap_values_df = bmap_sheet.copy()

    if selected_code_set_names:
        filtered_bmap_values_df = filtered_bmap_values_df[filtered_bmap_values_df['code set name'].isin(selected_code_set_names)]

    if selected_code_domain_names:
        filtered_bmap_values_df = filtered_bmap_values_df[filtered_bmap_values_df['code domain name'].isin(selected_code_domain_names)]

#----------------------------------------   Process Selected Action  -------------------------------------------------------
    if data_type == "int":
        bigint_flag = "0"
    elif data_type == "BIG int":
        bigint_flag = "1"

    key_set_id = bkey_sheet[bkey_sheet['key set name'] == selected_key_set]['key set id'].iloc[0] if selected_key_set else None
    Key_Domain_ID = bkey_sheet[bkey_sheet['key set name'] == selected_key_set]['key domain id'].iloc[0] if selected_key_set else None
# -----------------------------------------  Core tables  -------------------------------------------------------------------
    selected_core_table , selected_mapping_name = st.columns(2)
    with selected_core_table:
        selected_core_table = st.selectbox(
            "Select core table:", 
            options = core_tables_sheet['table name'].unique(),
            index=None,  # No initial selection
            placeholder="Choose an option..."  ,
            key="core_table_names",
            disabled=(tab_name != "core tables")  # Disable when CORE tables tab is not selected
        )
    filtered_core_tables_df = core_tables_sheet[core_tables_sheet['table name'] == selected_core_table]

    with selected_mapping_name:
        selected_mapping_name = st.selectbox(
        "Select mapping name:", 
        options = table_mapping_sheet[table_mapping_sheet['target table name'] == selected_core_table]['mapping name'].unique(),
        index=None,  # No initial selection
        placeholder="Choose an option..."  ,
        key="core_mapping_name",
        disabled=(tab_name != "core tables")  # Disable when CORE tables tab is not selected
    )

    # Filter table mapping by selected core table and mapping name
    filtered_table_mapping_df = table_mapping_sheet[
        (table_mapping_sheet['target table name'] == selected_core_table) & 
        (table_mapping_sheet['mapping name'] == selected_mapping_name)
    ]
    # filter column_mapping_sheet by selected_mapping_name
    filtered_column_mapping_df = column_mapping_sheet[column_mapping_sheet['mapping name'] == selected_mapping_name]

#---------------------------------------    Display query editor and execute query    ---------------------------------------
    Dict = {
            "bkey":filtered_bkey_df,
            "stg tables" :filterd_STG_tables_df,
            "stream" : Stream_sheet,
            "bmap values" : filtered_bmap_values_df,
            "bmap" : bmap_sheet,
            "core tables": filtered_core_tables_df,
            "table mapping": filtered_table_mapping_df,
            "column mapping": filtered_column_mapping_df,
        }
    print("---------------------  Main  --------------------------")
    smx_model = {k.lower(): v for k, v in Dict.items()}

    # Process each DataFrame: lowercase
    for key in smx_model:
        smx_model[key].columns = [col.lower() for col in smx_model[key].columns]

    # At the top of your script logic, after the button columns
    gen_query_col, export_query_col, exec_query_col = st.columns(3) 

    # Initialize session state if not exists
    if "generated_query" not in st.session_state:
        st.session_state["generated_query"] = ""
    with gen_query_col:
        if st.button(f"Generate Query", key=f"Generate_Query_Bttn"):
            script = generating_scripts.main(smx_model, selected_action, selected_environment, bigint_flag)
            
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
                # Ensure current_query_in_editor reflects the latest from the session_state before executing
                query_to_execute = current_query_in_editor
                st.success(f"Executed: {query_to_execute}")
#------------------------------------------------------------------------------------------------------------------------------
else:
    st.info("Please upload an Excel file to begin.")