from sqlalchemy.sql.functions import user
import streamlit as st
import numpy as np
import pandas as pd
from util import connect_to_cs as cs
from util.auth import check_authentication
import os
from pathlib import Path

# Authentication check - must be first command
authenticator = check_authentication()
# Show logout button
with st.sidebar:
    authenticator.logout('Logout', 'sidebar')
    

# Initialize session state
if 'db_list_data' not in st.session_state: # To store (DataFrame of databases, query string)
    st.session_state.db_list_data = None
if 'show_db_selection_ui' not in st.session_state: # If True, show the database selection UI
    st.session_state.show_db_selection_ui = True
if 'show_tables_data' not in st.session_state: # If True, show the tables data section
    st.session_state.show_tables_data = False
if 'selected_databases' not in st.session_state:
    st.session_state.selected_databases = []
if 'filtered_tables_df' not in st.session_state: # DataFrame of tables for selected DBs
    st.session_state.filtered_tables_df = pd.DataFrame()
if 'queries_for_filtered_tables' not in st.session_state: # Queries used to get filtered_tables_df
    st.session_state.queries_for_filtered_tables = []
# New session state variables for custom query
if 'custom_query_input' not in st.session_state:
    st.session_state.custom_query_input = ""
if 'custom_query_result_df' not in st.session_state:
    st.session_state.custom_query_result_df = None
if 'custom_query_error' not in st.session_state:
    st.session_state.custom_query_error = None
if 'show_custom_query_results' not in st.session_state:
    st.session_state.show_custom_query_results = False
# New session state variables for file editor
if 'editor_content' not in st.session_state:
    st.session_state.editor_content = ""


def list_tables_for_selected_dbs():
    all_tables_list = []
    executed_queries = []
    if st.session_state.selected_databases:
        for database_name in st.session_state.selected_databases:
            try:
                tables_df, q_executed = cs.list_objects(database_name)
                if tables_df is not None and not tables_df.empty:
                    all_tables_list.append(tables_df)
                if q_executed: # Ensure query is not None before appending
                    executed_queries.append(q_executed)
            except Exception as e:
                st.error(f"Error fetching tables for database {database_name}: {e}")
        
        if all_tables_list:
            combined_tables_df = pd.concat(all_tables_list, ignore_index=True)
            return combined_tables_df, executed_queries
        else:
            return pd.DataFrame(), executed_queries # Return empty DataFrame and queries if no tables found
    else:
        return pd.DataFrame(), []

def read_file_content(file_path):
    """Read and return file content with error handling"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except UnicodeDecodeError:
        try:
            # Try with different encoding
            with open(file_path, 'r', encoding='latin1') as file:
                return file.read()
        except Exception as e:
            return f"Error reading file {file_path}: {str(e)}"
    except Exception as e:
        return f"Error reading file {file_path}: {str(e)}"

# # --- Main app flow ---

# # 1. Fetch initial list of databases (once per session)
# if st.session_state.db_list_data is None:
#     try:
#         databases_df, query_str = cs.establish_TD_connection()
#         st.session_state.db_list_data = (databases_df, query_str)
#     except Exception as e:
#         st.error(f"Failed to connect to Vantage and retrieve database list: {e}")
#         # Ensure db_list_data is not None to prevent re-attempts, but holds error indication
#         st.session_state.db_list_data = (pd.DataFrame(columns=['DataBaseName']), f"Error: {e}")


# # 2. Display UI for database selection
# if st.session_state.show_db_selection_ui:
#     st.markdown("Used Query to select All TeraData Databases : ")  # Add another horizontal line

#     # Retrieve cached database list and query
#     data_df, query_for_dbs = st.session_state.db_list_data

#     if query_for_dbs and not query_for_dbs.startswith("Error:"):
#         st.code(query_for_dbs)
#     elif query_for_dbs and query_for_dbs.startswith("Error:"):
#         # Error message was shown during fetch when st.session_state.db_list_data was set
#         pass 
#     else:
#         st.info("Query for listing databases was not available.")
#     st.markdown("---")  # Add a horizontal line

#     if data_df is not None and not data_df.empty and 'DataBaseName' in data_df.columns:
#         available_databases = data_df['DataBaseName'].unique().tolist()
        
#         # Multiselect widget. Changing its value reruns the script. This is Streamlit's behavior.
#         st.session_state.selected_databases = st.multiselect(
#             "Select Databases to view its tables:",
#             options=available_databases,
#             # default=st.session_state.selected_databases # Persist selection across reruns
#         )
        
#         # Button to trigger table listing for selected databases
#         if st.button("List DB Tables"):
#             st.session_state.show_custom_query_results = False # Hide custom query results when listing tables
#             st.session_state.custom_query_result_df = None
#             st.session_state.custom_query_error = None
#             if st.session_state.selected_databases:
#                 # This block executes ONLY when the button is clicked.
#                 tables_df, queries_list = list_tables_for_selected_dbs()
#                 st.session_state.filtered_tables_df = tables_df
#                 st.session_state.queries_for_filtered_tables = queries_list
#                 st.session_state.show_tables_data = True # Set flag to display tables section

#                 if not queries_list and (tables_df is None or tables_df.empty):
#                      st.info("No tables found for the selected databases.")
#             else:
#                 st.warning("Please select at least one database first.")


#     elif not (query_for_dbs and query_for_dbs.startswith("Error:")): 
#         # If not an error state, but data_df is None, empty, or lacks DataBaseName column
#         st.warning("No database data found to select from. Ensure the connection is successful and returns data with a 'DataBaseName' column.")


# # 3. Display tables for selected databases (if button was clicked and data is available)
# if st.session_state.show_tables_data:
#     if st.session_state.filtered_tables_df is not None and not st.session_state.filtered_tables_df.empty:
#         # Optionally display the queries executed to get these tables
#         if st.session_state.queries_for_filtered_tables:
#             # Filter out None or empty strings from queries list before joining
#             valid_queries = [query for query in st.session_state.queries_for_filtered_tables if query]
#             if valid_queries:
#                 st.expander("Show Executed SQL Queries for Tables").code('\n---\n'.join(valid_queries))

#         # Group tables by 'DataBaseName' and iterate through selected databases to maintain order
#         # and ensure only selected databases are shown.
#         processed_databases = set()
#         for db_name_selected in st.session_state.selected_databases:
#             # Filter the DataFrame for the current selected database
#             db_specific_tables_df = st.session_state.filtered_tables_df[
#                 st.session_state.filtered_tables_df['DataBaseName'].str.strip() == db_name_selected.strip()
#             ]

#             if not db_specific_tables_df.empty:
#                 st.subheader(f"Database: {db_name_selected.strip()}")
#                 st.dataframe(
#                     db_specific_tables_df, # This DataFrame includes the 'DataBaseName' column
#                     use_container_width=True,
#                     hide_index=True
#                 )
#                 processed_databases.add(db_name_selected.strip())
                
#     else: # show_tables_data is True, but filtered_tables_df is empty or None
#         st.info("No tables to display for the selected databases, or an error occurred during fetching.")

# st.markdown("---")  # Add a horizontal line

# File Selection and Editor Section
st.subheader("📁 File Selection for Query Editor")

col1, col2 ,col3= st.columns(3)
with col1:
    selected_environment = st.selectbox(
        "Select Environment :",
        options=["Testing", "Development", "Production"],
        index=0,
        key="select_environment",
    )
with col2:
    # Get the project root directory and join with Exported Queries folder
    project_root = Path(__file__).parent.parent
    env_path = os.path.join(project_root, "Exported Queries", st.session_state.select_environment)
    # Find all version folders in the environment directory
    SMX_folders = []
    if os.path.exists(env_path):
        SMX_folders = [f.name for f in os.scandir(env_path) if f.is_dir()]
    # Sort folders to get the latest version (assuming version numbering in names)
    SMX_folders.sort(reverse=True)
    if SMX_folders:
        Latest_SMX_folder_path = os.path.join(env_path, SMX_folders[0])
        TabsFolders = [f.name for f in os.scandir(Latest_SMX_folder_path) if f.is_dir()]
    else:
        TabsFolders = []
    selected_tab = st.selectbox(
        "Select key area:",
        options=TabsFolders,
        index=0,
        key="tab_selector",
    )   
with col3:
    # Get the path for the selected tab
    selected_tab_path = os.path.join(Latest_SMX_folder_path, selected_tab)
    # Find all files in the selected tab folder
    dates_folders = [f.name for f in os.scandir(selected_tab_path) if f.is_dir()]
    # Sort folders to get the latest date
    dates_folders.sort(reverse=True)
    if dates_folders:
        latest_date_folder = os.path.join(selected_tab_path, dates_folders[0])
        # Find all Keytype folders
        Keytype_folders = [f.name for f in os.scandir(latest_date_folder) if f.is_dir()]    
    else:
        Keytype_folders = []

    selected_action = st.selectbox(
        f"Select Key type:",
        options = Keytype_folders,
        index=0,
        key="action" 
    )

# After getting the selected_action path
selected_action_path = os.path.join(latest_date_folder, selected_action)
# Get all files in the selected action folder that contain the username
username = st.session_state.get('username', '')
matching_files = []
print(username)
if os.path.exists(selected_action_path) and username:
    matching_files = [
        f.name for f in os.scandir(selected_action_path) 
        if f.is_file() and username in f.name
    ]

# Show files in multi-select
selected_files = st.multiselect(
    "Select files to work with:",
    options=matching_files,
    default=matching_files[0] if matching_files else None
)

# File operations section
if selected_files:    
    # Buttons for file operations
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("👁️ Preview Files"):
            st.subheader("📖 File Preview")
            for file_name in selected_files:
                file_path = os.path.join(selected_action_path, file_name)
                with st.expander(f"Preview: {file_name}"):
                    content = read_file_content(file_path)
                    st.text(content)
    
    with col2:
        if st.button("📝 Append to Editor"):
            for file_name in selected_files:
                file_path = os.path.join(selected_action_path, file_name)
                separator = "\n" "/*   "+"File Name : "+file_name+"    */" + "\n"
                st.session_state.editor_content += separator
                content = read_file_content(file_path)
                st.session_state.editor_content += content

            st.rerun()
    
    with col3:
        if st.button("🗑️ Clear Editor"):
            st.session_state.editor_content = ""
            st.success("🗑️ Editor cleared!")
            st.rerun()


# File Content Editor Section
st.markdown("---")
st.subheader("✏️ SQL Query Editor")

# Editor with current content
editor_content = st.text_area(
    "Edit your SQL queries here:",
    value=st.session_state.editor_content,
    height=400,
    key="sql_editor",
    help="You can edit the SQL content here. The content from selected files has been appended above."
)

# Update session state when editor content changes
if editor_content != st.session_state.editor_content:
    st.session_state.editor_content = editor_content
    st.session_state.custom_query_input = editor_content  # Sync with custom query input

# Editor statistics and download options
if st.session_state.editor_content:
    col1, col2, col3 , col4 = st.columns(4)
    
    with col1:
        st.metric("📝 Lines", len(st.session_state.editor_content.split('\n')))
    with col2:
        st.metric("📄 Files", st.session_state.editor_content.count("File Name :"))
    with col3:
        # Download button
        st.download_button(
            label="💾 Download SQL",
            data=st.session_state.editor_content,
            file_name=f"combined_queries_{username}.sql",
            mime="text/plain",
            help="Download the editor content as SQL file"
        )
    with col4:
        if st.button("⚡ Execute Content"):
            if st.session_state.editor_content.strip():
                with st.spinner("Executing query from editor..."):
                    try:
                        result_df, executed_query = cs.execute_custom_query(st.session_state.editor_content)
                        st.session_state.custom_query_result_df = result_df
                        st.session_state.custom_query_error = None
                        st.session_state.show_custom_query_results = True
                        st.success("✅ Editor query executed successfully!")
                    except Exception as e:
                        st.session_state.custom_query_error = f"Error executing editor query: {str(e)}"
                        st.session_state.custom_query_result_df = None
                        st.session_state.show_custom_query_results = True
