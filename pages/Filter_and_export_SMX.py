from matplotlib.backends.registry import backend_registry
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
import util.tab_operations as tab_operations

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
            key="tab_selector",
        ) 

    # Action / Query
    with col3:
        selected_action = st.selectbox(
            f"Key type:",
            options=tab_operations.get_action_options(selected_tab),
            key="Key type"
        )
    if selected_action:
        disable_statuses = tab_operations.get_all_disable_statuses(selected_action, selected_tab)
        
        # Replace your individual disable variables with:
        disable_frequency = disable_statuses["disable_frequency"]
        disable_data_type = disable_statuses["disable_data_type"] 
        disable_key_set = disable_statuses["disable_key_set"]
        disable_key_domain = disable_statuses["disable_key_domain"]
        disable_stg_tables = disable_statuses["disable_stg_tables"]
        disable_code_set_names = disable_statuses["disable_code_set_names"]
        disable_code_domain_names = disable_statuses["disable_code_domain_names"]
        disable_core_tables = disable_statuses["disable_core_tables"]
        disable_mapping_names = disable_statuses["disable_mapping_names"]
    else:
        # Default disable statuses when no action is selected
        disable_frequency = True
        disable_data_type = True
        disable_key_set = True
        disable_key_domain = True
        disable_stg_tables = True
        disable_code_set_names = True
        disable_code_domain_names = True
        disable_core_tables = True
        disable_mapping_names = True
    col1, col2 = st.columns(2)    
    # Key Set Name
    with col1:
        Frequency = st.selectbox(
            "Frequency",
            options =["Daily", "weekly" , "Monthly"],
            index=None,  # No initial selection 
            placeholder="Choose an option..."  ,
            key=f"{selected_action}Frequency",
            disabled=disable_frequency
        )
    with col2:
        selected_data_type = st.selectbox(
            "Data type flag :",
            options =["int", "BIG int"],
            index=None,  # No initial selection
            placeholder="Choose an option..."  ,
            key=f"{selected_action}data_type",
            disabled= disable_data_type 
        )
    if selected_data_type == "int":
        bigint_flag = 0
    elif selected_data_type == "BIG int":
        bigint_flag = 1
    else :
        bigint_flag=None


# --------------------------------------------------- key set names ------------------------------------------------

    multi_col3 , multi_col1, multi_col2  = st.columns(3)  
    with multi_col3:
        # Get base options
        all_key_set_names = bkey_sheet['key set name'].replace('', pd.NA).dropna().unique().tolist()
        selected_key_set = st.selectbox( 
            "Key Set Name (bkey sheet):", 
            options = ["All"] + all_key_set_names,
            index=None,  
            placeholder="Choose an option..."  ,    
            key=f"{selected_action}key_set_name",
            disabled = disable_key_set  
        )
        if selected_key_set =="All":
            selected_key_set = all_key_set_names
        else: 
            selected_key_set = [selected_key_set]

# --------------------------------------------------- Key domain name ------------------------------------------------
    # Filtered BKEY sheet By Key Set Name
    filtered_key_set_names_DF = bkey_sheet[bkey_sheet['key set name'].isin(selected_key_set)] 

    # Key Domain Name  

    with multi_col1:
        all_key_domains = filtered_key_set_names_DF['key domain name'].dropna().unique().tolist() 
        selected_domains = st.selectbox(
            "Key Domain (bkey sheet):",
            options=["All"] + all_key_domains,
            index=None,  
            placeholder="Choose an option..."  ,
            key=f"{selected_action}key_domain_name",
            help="""- this domain are filtered based on selected key set name
- if you didn't select any domain, all domains will be selected """,
        disabled= disable_key_domain
        )
    
    selected_domains = all_key_domains if selected_domains == "All" else [selected_domains]

# --------------------------------------------------- STG tables ------------------------------------------------
    # filtered bkey df based on key set name && key domain name
    if (selected_domains and not disable_key_domain):   
        filtered_bkey_df = filtered_key_set_names_DF[filtered_key_set_names_DF['key domain name'].isin(selected_domains)]
    else:
        filtered_bkey_df = filtered_key_set_names_DF

    # STG Tables 
    with multi_col2:
        # Get all STG table options
        all_stg_tables = stg_tables_sheet['table name stg'].dropna().unique().tolist() 
        # Filter STG tables based on selected key set 
        filtered_stg_table = stg_tables_sheet
        stg_table_options=[]
        # This ensure also that stg tables DF has no null in the key set name and key domain name 
        if disable_key_set and disable_key_domain:
            stg_table_options = all_stg_tables
        elif not disable_key_set and not disable_key_domain and selected_key_set and selected_domains:
            filtered_stg_table = stg_tables_sheet[stg_tables_sheet['key set name'].isin(selected_key_set)]
            filtered_stg_table = filtered_stg_table[filtered_stg_table['key domain name'].isin(selected_domains)]
            stg_table_options = filtered_stg_table['table name stg'].dropna().unique().tolist()

        selected_tables = st.selectbox(
            "STG table :",
            options=["All"] + stg_table_options,
            index=None,  
            placeholder="Choose an option..."  ,
            help="""- This tables are filtered based on selected key set name and code domain names """,
            key=f"{selected_action}stg_tables",
            disabled= disable_stg_tables
        ) 
        selected_tables = all_stg_tables  if selected_tables == "All" else [selected_tables]

    # Filter STG tables dataframe
    filterd_STG_tables_df = filtered_stg_table[filtered_stg_table['table name stg'].isin(selected_tables)] if selected_tables else stg_tables_sheet
# --------------------------------------------------- code_set_names  ------------------------------------------------



    multi_coll1, multi_coll2 = st.columns(2)    
    with multi_coll1:
        # Show unique values from bmap_sheet for selection
        all_code_set_names = bmap_sheet['code set name'].unique().tolist()
        selected_code_set_names = st.selectbox(
            "Select Code Set Name:",
            options=["All"] + all_code_set_names,
            key=f"{selected_action}code_set_names",
            disabled=disable_code_set_names
        )
    selected_code_set_names = all_code_set_names if selected_code_set_names == "All" else [selected_code_set_names]

# --------------------------------------------------- code_domain_names  ------------------------------------------------

    filtered_code_set_names_df = bmap_values_sheet[bmap_values_sheet['code set name'].isin(selected_code_set_names)]
    code_domain_names_options = filtered_code_set_names_df['code domain name'].unique().tolist()

    with multi_coll2:
        
        selected_code_domain_names = st.selectbox(
            "Select Code Domain Name:",
            options=["All"] + code_domain_names_options,
            help="""- This options are filtered by the selected code set name""",
            key=f"{selected_action}code_domain_names",
            disabled= disable_code_domain_names
        )
        
    selected_code_domain_names = code_domain_names_options if selected_code_domain_names == "All" else [selected_code_domain_names]

# -----------------------------------------  Core tables  -------------------------------------------------------------------
    selected_core_table , selected_mapping_name = st.columns(2)
    with selected_core_table:
        # core tables without lookups (core table that doesn't have subject area )
        core_tables_options = core_tables_sheet.replace('', pd.NA).dropna(subset=['subject area'])['table name'].unique().tolist()
        
        selected_core_table = st.selectbox(
            "Select core table:", 
            options = ["All"] + core_tables_options,
            index=None,  
            placeholder="Choose an option..."  ,
            key=f"{selected_action}core_table_names",
            disabled=  disable_core_tables # Disable when CORE tables tab is not selected
        )
        
    selected_core_table = core_tables_options if selected_core_table == "All" else [selected_core_table]

    with selected_mapping_name:
        if (selected_core_table and not disable_core_tables):
            all_mapping_names = table_mapping_sheet[table_mapping_sheet['target table name'].isin(selected_core_table)]['mapping name'].unique().tolist() 
        else:
            all_mapping_names =table_mapping_sheet['mapping name'].unique().tolist()
        selected_mapping_name = st.selectbox(
        "Select mapping name:", 
        options = ["All"] + all_mapping_names,
        index=None,  # No initial selection
        placeholder="Choose an option..."  ,
        key=f"{selected_action}core_mapping_name",
        disabled= disable_mapping_names # Disable when CORE tables tab is not selected
    )
    
    selected_mapping_name = all_mapping_names if selected_mapping_name == "All" else [selected_mapping_name]

    # Filter table mapping by selected core table and mapping name
    filtered_table_mapping_df = table_mapping_sheet[
        (table_mapping_sheet['target table name'].isin(selected_core_table)) & 
        (table_mapping_sheet['mapping name'].isin(selected_mapping_name))
    ]
    # filter column_mapping_sheet by selected_mapping_name
    filtered_column_mapping_df = column_mapping_sheet[column_mapping_sheet['mapping name'].isin(selected_mapping_name)]

#---------------------------------------    Display query editor and execute query    ---------------------------------------
    if disable_key_set and disable_key_domain:
        filtered_bkey_df = bkey_sheet

    filtered_core_tables_df = core_tables_sheet if not selected_core_table else core_tables_sheet[core_tables_sheet['table name'].isin(selected_core_table)]
    
    # filtered bmap values by code domain name && code set name
    filtered_bmap_values_df = filtered_code_set_names_df[filtered_code_set_names_df['code domain name'].isin(selected_code_domain_names)]
    
    # filtered bmap df by code domain name && code set name
    filtered_bmap_df = bmap_sheet[bmap_sheet['code domain name'].isin(selected_code_domain_names)]
    filtered_bmap_df = filtered_bmap_df[filtered_bmap_df['code set name'].isin(selected_code_set_names)]
#--------------------------------------------   Filter Stream   ------------------------------------------ 
    filtered_stream = Stream_sheet[Stream_sheet['system name'].isin(filterd_STG_tables_df['source system alias'])]
    # Extract substrings after underscore
    stream_name = filtered_stream['stream name'].str.split('_').str[1]
    # Check which substrings are contained in your input string
    mask = stream_name.str.lower().apply(lambda x: x in selected_tab.lower())
    filtered_stream = filtered_stream[mask]

    Dict = {
            "bkey":filtered_bkey_df,
            "stg tables" :filterd_STG_tables_df ,
            "stream" : filtered_stream,
            "bmap values" : filtered_bmap_values_df,
            "bmap" : filtered_bmap_df,
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
            
            # Simple validation check
            is_valid, validation_message = tab_operations.validate_all_required_fields(
                selected_action, 
                selected_tab,
                selected_environment=selected_environment,
                selected_data_type=selected_data_type,
                selected_key_set=selected_key_set,
                selected_domains=selected_domains,
                selected_tables=selected_tables,
                selected_code_set_names=selected_code_set_names,
                selected_code_domain_names=selected_code_domain_names,
                selected_core_table=selected_core_table,
                selected_mapping_name=selected_mapping_name
            )
            
            if not is_valid:
                st.warning(validation_message)
            else:
                # Proceed with query generation
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
    # Use st_ace for SQL syntax highlighting
    current_query_in_editor = st.text_area(
        "Query:",   
        value=st.session_state["generated_query"],
        height=500,
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
                tab_folder_path = os.path.join(excel_folder_path, selected_tab)
                
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