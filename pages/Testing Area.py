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

# File Selection and Editor Section
st.subheader("📁 File Selection for Query Editor")

col1, col2, col3 = st.columns(3)

with col1:
    selected_environment = st.selectbox(
        "Select Environment:",
        options=["Testing", "Development", "Production"],
        index=0,
        key="select_environment",
    )

# Get the project root directory and join with Exported Queries folder
project_root = Path(__file__).parent.parent
env_mapping = {
    "Testing": "TST",  # Fixed: Changed from "Testing" to "TST" to match your file path
    "Development": "DEV", 
    "Production": "Production"
}
env_path = os.path.join(project_root, "Exported Queries", env_mapping[selected_environment])

# Debug: Show the constructed path
st.write(f"Environment path: {env_path}")

# Initialize variables
SMX_folders = []
Latest_SMX_folder_path = None
TabsFolders = []
dates_folders = []
latest_date_folder = None
Keytype_folders = []

# Find all version folders in the environment directory
if os.path.exists(env_path):
    SMX_folders = [f.name for f in os.scandir(env_path) if f.is_dir()]
    # Sort folders to get the latest version (assuming version numbering in names)
    SMX_folders.sort(reverse=True)
    st.write(f"Found SMX folders: {SMX_folders}")  # Debug output
else:
    st.error(f"Environment path does not exist: {env_path}")

if SMX_folders:
    Latest_SMX_folder_path = os.path.join(env_path, SMX_folders[0])
    st.write(f"Latest SMX folder path: {Latest_SMX_folder_path}")  # Debug output
    
    if os.path.exists(Latest_SMX_folder_path):
        TabsFolders = [f.name for f in os.scandir(Latest_SMX_folder_path) if f.is_dir()]
        st.write(f"Found Tab folders: {TabsFolders}")  # Debug output
    else:
        st.error(f"Latest SMX folder path does not exist: {Latest_SMX_folder_path}")

with col2:
    selected_tab = st.selectbox(
        "Select key area:",
        options=TabsFolders if TabsFolders else ["No folders found"],
        index=0,
        key="tab_selector",
        disabled=not bool(TabsFolders)
    )

# Only proceed if we have valid paths
if Latest_SMX_folder_path and selected_tab != "No folders found":
    # Get the path for the selected tab
    selected_tab_path = os.path.join(Latest_SMX_folder_path, selected_tab)
    st.write(f"Selected tab path: {selected_tab_path}")  # Debug output
    
    if os.path.exists(selected_tab_path):
        # Find all date folders in the selected tab folder
        dates_folders = [f.name for f in os.scandir(selected_tab_path) if f.is_dir()]
        # Sort folders to get the latest date
        dates_folders.sort(reverse=True)
        st.write(f"Found date folders: {dates_folders}")  # Debug output
        
        if dates_folders:
            latest_date_folder = os.path.join(selected_tab_path, dates_folders[0])
            st.write(f"Latest date folder: {latest_date_folder}")  # Debug output
            
            if os.path.exists(latest_date_folder):
                # Find all Keytype folders
                Keytype_folders = [f.name for f in os.scandir(latest_date_folder) if f.is_dir()]
                st.write(f"Found Keytype folders: {Keytype_folders}")  # Debug output
            else:
                st.error(f"Latest date folder does not exist: {latest_date_folder}")
    else:
        st.error(f"Selected tab path does not exist: {selected_tab_path}")

with col3:
    selected_action = st.selectbox(
        "Select Key type:",
        options=Keytype_folders if Keytype_folders else ["No folders found"],
        index=0,
        key="action",
        disabled=not bool(Keytype_folders)
    )

# File selection section
matching_files = []
selected_action_path = None

if (latest_date_folder and selected_action != "No folders found" and 
    selected_action in Keytype_folders):
    # After getting the selected_action path
    selected_action_path = os.path.join(latest_date_folder, selected_action)
    st.write(f"Selected action path: {selected_action_path}")  # Debug output

    # Get username from session state
    username = st.session_state.get('username', '')
    st.write(f"Username from session: '{username}'")  # Debug output
    
    if os.path.exists(selected_action_path):
        # Get all files first for debugging
        all_files = [f.name for f in os.scandir(selected_action_path) if f.is_file()]
        st.write(f"All files in directory: {all_files}")  # Debug output
        
        if username:
            # Show files that contain username
            matching_files = [
                f.name for f in os.scandir(selected_action_path) 
                if f.is_file() and username.lower() in f.name.lower()  # Case-insensitive match
            ]
            st.write(f"Files matching username '{username}': {matching_files}")  # Debug output
        else:
            st.warning("Username not found in session state. Please check authentication.")
            # Show all files if no username
            matching_files = all_files
    else:
        st.error(f"Selected action path does not exist: {selected_action_path}")

# Show files in multi-select
selected_files = st.multiselect(
    "Select files to work with:",
    options=matching_files if matching_files else ["No matching files found"],
    default=matching_files[0:1] if matching_files else [],  # Fixed: Use list slice instead of single item
    disabled=not bool(matching_files)
)

# File operations section
if selected_files and selected_files != ["No matching files found"] and selected_action_path:    
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
                separator = "\n" + "/*   " + "File Name : " + file_name + "    */" + "\n"
                st.session_state.editor_content += separator
                content = read_file_content(file_path)
                st.session_state.editor_content += content + "\n"  # Add newline after content

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
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📝 Lines", len(st.session_state.editor_content.split('\n')))
    with col2:
        st.metric("📄 Files", st.session_state.editor_content.count("File Name :"))
    with col3:
        # Get username for download filename
        username = st.session_state.get('username', 'user')
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
            else:
                st.warning("⚠️ Editor content is empty. Please add some SQL queries first.")