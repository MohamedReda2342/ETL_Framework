import streamlit as st
import numpy as np
import pandas as pd
from util import connect_to_cs as cs

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

# --- Main app flow ---

# 1. Fetch initial list of databases (once per session)
if st.session_state.db_list_data is None:
    try:
        databases_df, query_str = cs.establish_TD_connection()
        st.session_state.db_list_data = (databases_df, query_str)
    except Exception as e:
        st.error(f"Failed to connect to Vantage and retrieve database list: {e}")
        # Ensure db_list_data is not None to prevent re-attempts, but holds error indication
        st.session_state.db_list_data = (pd.DataFrame(columns=['DataBaseName']), f"Error: {e}")


# 2. Display UI for database selection
if st.session_state.show_db_selection_ui:
    st.markdown("Used Query to select All TeraData Databases : ")  # Add another horizontal line

    # Retrieve cached database list and query
    data_df, query_for_dbs = st.session_state.db_list_data

    if query_for_dbs and not query_for_dbs.startswith("Error:"):
        st.code(query_for_dbs)
    elif query_for_dbs and query_for_dbs.startswith("Error:"):
        # Error message was shown during fetch when st.session_state.db_list_data was set
        pass 
    else:
        st.info("Query for listing databases was not available.")
    st.markdown("---")  # Add a horizontal line

    if data_df is not None and not data_df.empty and 'DataBaseName' in data_df.columns:
        available_databases = data_df['DataBaseName'].unique().tolist()
        
        # Multiselect widget. Changing its value reruns the script. This is Streamlit's behavior.
        st.session_state.selected_databases = st.multiselect(
            "Select Databases to view its tables:",
            options=available_databases,
            # default=st.session_state.selected_databases # Persist selection across reruns
        )
        
        # Button to trigger table listing for selected databases
        if st.button("List DB Tables"):
            st.session_state.show_custom_query_results = False # Hide custom query results when listing tables
            st.session_state.custom_query_result_df = None
            st.session_state.custom_query_error = None
            if st.session_state.selected_databases:
                # This block executes ONLY when the button is clicked.
                tables_df, queries_list = list_tables_for_selected_dbs()
                st.session_state.filtered_tables_df = tables_df
                st.session_state.queries_for_filtered_tables = queries_list
                st.session_state.show_tables_data = True # Set flag to display tables section

                if not queries_list and (tables_df is None or tables_df.empty):
                     st.info("No tables found for the selected databases.")
            else:
                st.warning("Please select at least one database first.")


    elif not (query_for_dbs and query_for_dbs.startswith("Error:")): 
        # If not an error state, but data_df is None, empty, or lacks DataBaseName column
        st.warning("No database data found to select from. Ensure the connection is successful and returns data with a 'DataBaseName' column.")


# 3. Display tables for selected databases (if button was clicked and data is available)
if st.session_state.show_tables_data:
    if st.session_state.filtered_tables_df is not None and not st.session_state.filtered_tables_df.empty:
        # Optionally display the queries executed to get these tables
        if st.session_state.queries_for_filtered_tables:
            # Filter out None or empty strings from queries list before joining
            valid_queries = [query for query in st.session_state.queries_for_filtered_tables if query]
            if valid_queries:
                st.expander("Show Executed SQL Queries for Tables").code('\n---\n'.join(valid_queries))

        # Group tables by 'DataBaseName' and iterate through selected databases to maintain order
        # and ensure only selected databases are shown.
        processed_databases = set()
        for db_name_selected in st.session_state.selected_databases:
            # Filter the DataFrame for the current selected database
            db_specific_tables_df = st.session_state.filtered_tables_df[
                st.session_state.filtered_tables_df['DataBaseName'].str.strip() == db_name_selected.strip()
            ]

            if not db_specific_tables_df.empty:
                st.subheader(f"Database: {db_name_selected.strip()}")
                st.dataframe(
                    db_specific_tables_df, # This DataFrame includes the 'DataBaseName' column
                    use_container_width=True,
                    hide_index=True
                )
                processed_databases.add(db_name_selected.strip())
                
    else: # show_tables_data is True, but filtered_tables_df is empty or None
        st.info("No tables to display for the selected databases, or an error occurred during fetching.")

st.markdown("---")  # Add a horizontal line

# Free text area for custom SQL query
st.subheader("Execute Custom SQL Query")

# Add a try-except block around the text area to handle any UI-related errors
try:
    st.session_state.custom_query_input = st.text_area(
        "Enter your SQL query here:",
        value=st.session_state.custom_query_input,
        height=150
    )
except Exception as ui_error:
    st.error(f"Error displaying query input area: {str(ui_error)}")
    st.session_state.custom_query_input = ""  # Reset to prevent cascading errors

# Button to execute the custom query
if st.button("Execute Query"):
    if st.session_state.custom_query_input.strip():
        # Show a spinner while query is executing
        with st.spinner("Executing query..."):
            try:
                # Execute the query using the connect_to_cs module
                result_df, executed_query = cs.execute_custom_query(st.session_state.custom_query_input)
                st.session_state.custom_query_result_df = result_df
                st.session_state.custom_query_error = None
                st.session_state.show_custom_query_results = True
                st.success("Query executed successfully!")
            except teradatasql.DatabaseError as db_err:
                # Handle specific database errors with more detailed messages
                error_message = str(db_err)
                if "syntax error" in error_message.lower():
                    st.session_state.custom_query_error = f"SQL Syntax Error: {error_message}"
                elif "permission" in error_message.lower() or "access" in error_message.lower():
                    st.session_state.custom_query_error = f"Permission Error: You may not have access to this data. {error_message}"
                elif "timeout" in error_message.lower():
                    st.session_state.custom_query_error = f"Query Timeout: The query took too long to execute. Try simplifying it. {error_message}"
                else:
                    st.session_state.custom_query_error = f"Database Error: {error_message}"
                
                st.session_state.custom_query_result_df = None
                st.session_state.show_custom_query_results = True
            except ConnectionError as conn_err:
                # Handle connection issues
                st.session_state.custom_query_error = f"Connection Error: Could not connect to the database. Please check your connection. {str(conn_err)}"
                st.session_state.custom_query_result_df = None
                st.session_state.show_custom_query_results = True
            except Exception as e:
                # Generic error handler as fallback
                st.session_state.custom_query_error = f"Error executing query: {str(e)}"
                st.session_state.custom_query_result_df = None
                st.session_state.show_custom_query_results = True
    else:
        st.warning("Please enter an SQL query to execute.")

# Display custom query results with enhanced error handling
if st.session_state.show_custom_query_results:
    try:
        st.subheader("Query Results")
        if st.session_state.custom_query_error:
            st.error(st.session_state.custom_query_error)
            # Provide helpful suggestions based on error type
            if "syntax error" in st.session_state.custom_query_error.lower():
                st.info("💡 Tip: Check your SQL syntax. Make sure all keywords, table names, and field names are correct.")
            elif "permission" in st.session_state.custom_query_error.lower():
                st.info("💡 Tip: You may not have access to this database or table. Try selecting from tables you have permission to access.")
        elif st.session_state.custom_query_result_df is not None:
            try:
                if not st.session_state.custom_query_result_df.empty:
                    # Add download button for query results
                    csv = st.session_state.custom_query_result_df.to_csv(index=False)
                    st.download_button(
                        label="Download Results as CSV",
                        data=csv,
                        file_name="query_results.csv",
                        mime="text/csv",
                    )
                    # Display the dataframe with error handling
                    st.dataframe(
                        st.session_state.custom_query_result_df, 
                        use_container_width=True
                    )
                else:
                    st.info("The query executed successfully but returned no data.")
            except Exception as df_error:
                st.error(f"Error displaying results: {str(df_error)}")
    except Exception as ui_render_error:
        st.error(f"Error rendering results UI: {str(ui_render_error)}")

