import streamlit as st
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
    sheet_names = df_utlis.get_excel_sheet_names(file_content)
    excel_file_name = uploaded_file.name.split('.')[0] # Get Excel file name without extension

    col1, col2 ,col3= st.columns(3)
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
            options=sheet_names,
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
            disabled=(tab_name != "BKEY")  # Disable when BKEY tab is selected
        )

    with col3:
        key_set_names ,bkey_sheet =tab_operations.get_key_set_options(file_content , selected_tables=[])
        selected_key_set = st.selectbox( 
        "Select Key Set Name:", 
        options = key_set_names,
        key="key_set_name",
        disabled=(tab_name != "BKEY")  # Disable when BKEY tab is selected
    )
    filtered_key_set_names_DF = df_utlis.filter_by_column_value(bkey_sheet, 'Key Set Name', selected_key_set)
    # Filtered STG tables By Key Set Name
    tables_names , STG_tables_df = tab_operations.get_stg_table_options(file_content, selected_key_set)
    # key domain name Column in ( STG Tables ) which is filtered already by key set name 
    key_domain_names = df_utlis.get_unique_values(STG_tables_df, 'Key Domain Name')

    multi_col1, multi_col2 = st.columns(2)

    # Key Domain Name 
    with multi_col1:
        selected_domains = st.multiselect(
            "Select Key Domain :",
            options=key_domain_names,
            key="key_domain_name" 
        )
    # filtered bkey df based on key set name && key domain name
    filtered_bkey_df = df_utlis.filter_by_column_value(filtered_key_set_names_DF, 'Key Domain Name', selected_domains)
    # STG Tables 
    with multi_col2:
        selected_tables = st.multiselect(
            "Select Table :",
            options= tables_names,
            key="stg_tables" 
        )
        filterd_STG_tables_df = df_utlis.filter_by_column_value(STG_tables_df, 'Table Name Source', selected_tables)
    


#----------------------------------------   Process Selected Action  -------------------------------------------------------


    if data_type == "int":
        bigint_flag = "0"
    elif data_type == "BIG int":
        bigint_flag = "1"

    key_set_id = bkey_sheet[bkey_sheet['Key Set Name'] == selected_key_set]['Key Set ID'].iloc[0]
    Key_Domain_ID = bkey_sheet[bkey_sheet['Key Set Name'] == selected_key_set]['Key Domain ID'].iloc[0]

    # if selected_action:
    #     if tab_name == "System":
    #         if selected_action == "register_system":
    #             query_for_editor = Queries.register_system(Ctl_Id="",source_system_alias="",Path_Name="",source_system_name="")

    #     elif tab_name == "Stream":
    #         if selected_action == "register_stream":
    #             query_for_editor = Queries.register_stream(Business_Date="",Cycle_Freq_Code="",stream_key="",stream_name="")
        
#---------------------------------------    Display query editor and execute query    ---------------------------------------
    
    # Workbook Should contain  Stream sheet
    Dict = {
        "BKEY" : filtered_bkey_df,
        "STG tables" : filterd_STG_tables_df,
        "Stream" : df_utlis.load_sheet(file_content, "Stream")
    }

    st.write(filterd_STG_tables_df)
    print("------Main---------")
    smx_model = {k.lower(): v for k, v in Dict.items()}

    # Process each DataFrame: lowercase and remove spaces from column names
    for key in smx_model:
        smx_model[key].columns = [col.lower() for col in smx_model[key].columns]


    # At the top of your script logic, after the button columns
    gen_query_col, export_query_col, exec_query_col = st.columns(3) 

    # Initialize session state if not exists
    if "generated_query" not in st.session_state:
        st.session_state["generated_query"] = ""
    with gen_query_col:
        if st.button(f"Generate Query", key=f"Generate_Query_Bttn"):
            # Check if all required selections are made
            if not (selected_domains and selected_tables and selected_action and selected_environment and selected_key_set and Frequency) :
                st.error("Please complete selections")
            else:
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
