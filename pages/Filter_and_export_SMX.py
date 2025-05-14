import streamlit as st
import pandas as pd
import openpyxl
import io

# Initialize session state for file storage
if 'data_file_content' not in st.session_state:
    st.session_state.data_file_content = None
if 'system_data' not in st.session_state:
    st.session_state.system_data = None
if 'sheet_names' not in st.session_state:
    st.session_state.sheet_names = None

# Cache the Excel file reading with TTL set to None (never expire)
@st.cache_data(ttl=None)
def load_excel_sheet(file_content, sheet_name):
    return pd.read_excel(io.BytesIO(file_content), sheet_name)

@st.cache_data(ttl=None)
def get_excel_sheet_names(file_content):
    return pd.ExcelFile(io.BytesIO(file_content)).sheet_names

# File uploader
uploaded_file = st.file_uploader("Upload Excel file", type=['xlsx'])

# Store the file in session state when uploaded
if uploaded_file is not None:
    # Read the file content once
    file_content = uploaded_file.getvalue()
    st.session_state.data_file_content = file_content
    
    # Get sheet names
    if st.session_state.sheet_names is None:
        st.session_state.sheet_names = get_excel_sheet_names(file_content)
    
    # Load system data once
    if st.session_state.system_data is None and "System" in st.session_state.sheet_names:
        system_df = load_excel_sheet(file_content, "System")
        # Clean data - remove rows with empty Source System Alias and invalid rows
        st.session_state.system_data = system_df.dropna(subset=['Source System Alias'])
        st.session_state.system_data = st.session_state.system_data[
            st.session_state.system_data['Source System Alias'].str.strip().astype(bool)
        ]

# Use the cached file from session state
if st.session_state.data_file_content is not None:
    file_content = st.session_state.data_file_content
    
    # System selection
    selected_system = None
    if st.session_state.system_data is not None and not st.session_state.system_data.empty:
        system_aliases = tuple(st.session_state.system_data['Source System Alias'].unique())
        selected_system = st.selectbox(
            "Select Source System", 
            system_aliases, 
            index=None,
            placeholder="Choose a system..."
        )
# ----------------------------------------------------------------------------------------------------------------------------------
        # STG tables  Section
        try:
            # Cache the STG tables data
            stg_df = load_excel_sheet(file_content, "STG tables")
            stg_filtered = stg_df[stg_df['Source System Alias'] == selected_system] if selected_system else stg_df
            
            st.subheader("STG tables")
            
            # Get unique table names and allow selection
            tables_available = stg_filtered['Table Name Source'].dropna().unique().tolist()
            
            if tables_available:
                selected_tables = st.multiselect(
                    "Select tables to display from (Table Name Source Tab):",
                    options=tables_available,
                    default=None  # Start with none selected
                )
                
                # Only show selected tables
                if selected_tables:
                    filtered_data = stg_filtered[stg_filtered['Table Name Source'].isin(selected_tables)]
                    # Group by table and display
                    for table_name, group in filtered_data.groupby('Table Name Source'):
                        # Hide the column we grouped by
                        display_group = group.drop(columns=['Table Name Source'])
                        
                        st.write(f"**Table: {table_name}**")
                        st.write(f"Full Table Name: {display_group['Table Name STG'].iloc[0]}")
                        
                        # Add buttons side by side using columns
                        col1, col2 = st.columns(2)
                        with col1:
                            st.button("Generate Query", key=f"gen_query_stg_{table_name}")
                        with col2:
                            st.button("Execute Query", key=f"exec_query_stg_{table_name}")
                            
                        st.dataframe(display_group.reset_index(drop=True))
                else:
                    st.write("Select tables from the dropdown above to view details")
            else:
                st.write("No STG tables found for selected system")
                
        except Exception as e:
            st.error(f"Error loading STG tables: {str(e)}")
# ----------------------------------------------------------------------------------------------------------------------------------
        # BKEY Data  Section
        try:
            sheet_names = get_excel_sheet_names(file_content)
            if "BKEY" in sheet_names:
                bkey_df = load_excel_sheet(file_content, "BKEY")
                
                st.subheader("BKEY Table")
                
                # Show description to help users understand the relationship
                st.info("The BMAP data shown here will be filtered based on your table selection 👆  \n"
                                "using both (Key Set Name && Key Domain Name)")                
                # Filter based on selected STG tables if any are selected
                if selected_tables and not bkey_df.empty:
                    # Get key_set_name and key_domain_name pairs from selected STG tables
                    selected_stg_data = stg_filtered[stg_filtered['Table Name Source'].isin(selected_tables)]
                    
                    # Get unique pairs from STG data
                    key_pairs = set(zip(selected_stg_data['Key Set Name'].dropna(), 
                                        selected_stg_data['Key Domain Name'].dropna()))

                    # Filter BKEY data based on these pairs
                    bkey_filtered = bkey_df[bkey_df[['Key Set Name', 'Key Domain Name']].apply(tuple, axis=1).isin(key_pairs)]
                    
                    if not bkey_filtered.empty:
                        # Add buttons side by side using columns
                        col1, col2 = st.columns(2)
                        with col1:
                            st.button("Generate Query", key="gen_query_bkey")
                        with col2:
                            st.button("Execute Query", key="exec_query_bkey")
                            
                        st.dataframe(bkey_filtered.dropna(how='all'))
                    else:
                        st.write("No BKEY mappings found for the selected tables.")
                else:
                    # Show empty placeholder or sample structure
                    if not bkey_df.empty:
                        # Add buttons side by side using columns
                        col1, col2 = st.columns(2)
                        with col1:
                            st.button("Generate Query", key="gen_query_bkey_empty")
                        with col2:
                            st.button("Execute Query", key="exec_query_bkey_empty")
                            
                        # Display empty dataframe with column headers to show structure
                        st.dataframe(bkey_df.head(0))
        except Exception as e:
            st.error(f"Error loading BKEY data: {str(e)}")
# ----------------------------------------------------------------------------------------------------------------------------------
        # BMAP Data  Section
        #  after table selection: Capture composite keys from selected STG tables
        selected_composite_keys = set()
        if selected_tables:
            # Get selected STG data
            selected_stg_data = stg_filtered[stg_filtered['Table Name Source'].isin(selected_tables)]
            # Extract valid composite keys
            selected_composite_keys = set(
                selected_stg_data[['Code Set Name', 'Code Domain Name']]
                .dropna()
                .apply(tuple, axis=1)
            )
        elif not selected_system:  # When no system selected but showing all STG tables
            # Get all composite keys from all STG tables
            selected_composite_keys = set(
                stg_df[['Code Set Name', 'Code Domain Name']]
                .dropna()
                .apply(tuple, axis=1)
            )

        try:
            if "BMAP" in sheet_names:
                bmap_df = load_excel_sheet(file_content, "BMAP")
                
                st.subheader("BMAP Mapping")
                # Show description to help users understand the relationship
                st.info("The BMAP data shown here will be filtered based on your table selection 👆  \n"
                                "using both (Code Set Name && Code Domain Name)")                  
                # Filter BMAP based on composite keys if any exist
                if selected_tables and not bmap_df.empty:
                    # Get selected STG data
                    selected_stg_data = stg_filtered[stg_filtered['Table Name Source'].isin(selected_tables)]
                    
                    # Get unique pairs from STG data
                    code_pairs = set(zip(selected_stg_data['Code Set Name'].dropna(), 
                                         selected_stg_data['Code Domain Name'].dropna()))
                    
                    # Filter BMAP data based on these pairs
                    bmap_filtered = bmap_df[bmap_df[['Code Set Name', 'Code Domain Name']].apply(tuple, axis=1).isin(code_pairs)]
                    
                    if not bmap_filtered.empty:
                        # Add buttons side by side using columns
                        col1, col2 = st.columns(2)
                        with col1:
                            st.button("Generate Query", key="gen_query_bmap")
                        with col2:
                            st.button("Execute Query", key="exec_query_bmap")
                            
                        st.dataframe(bmap_filtered.dropna(how='all'))
                    else:
                        st.write("No BMAP mappings found for the selected tables.")
                else:
                    # Show empty placeholder or sample structure
                    if not bmap_df.empty:
                        # Add buttons side by side using columns
                        col1, col2 = st.columns(2)
                        with col1:
                            st.button("Generate Query", key="gen_query_bmap_empty")
                        with col2:
                            st.button("Execute Query", key="exec_query_bmap_empty")
                            
                        # Display empty dataframe with column headers to show structure
                        st.dataframe(bmap_df.head(0))
        except Exception as e:
            st.error(f"Error loading BMAP data: {str(e)}")
# ----------------------------------------------------------------------------------------------------------------------------------
        # BMAP Values Section
        try:
            if "BMAP values" in sheet_names:
                bmapv_df = load_excel_sheet(file_content, "BMAP values")
                
                st.subheader("BMAP values")
                # Show description to help users understand the relationship
                st.info("The BMAP Values data shown here will be filtered based on the BMAP table above.👆  \n"
                                "using both (Code Set Name && Code domain ID)")  
                
                # Filter based on BMAP table
                if 'bmap_filtered' in locals() and not bmap_filtered.empty:
                    # Create composite keys from filtered BMAP
                    bmap_composite = set(zip(bmap_filtered['Code domain ID'], bmap_filtered['Code Set Name']))
                    
                    # Create composite keys in BMAP Values
                    bmapv_df['Composite_Key'] = list(zip(bmapv_df['Code domain ID'], 
                                                        bmapv_df['Code Set Name']))

                    # Filter BMAP Values based on composite keys
                    bmapv_filtered = bmapv_df[bmapv_df['Composite_Key'].isin(bmap_composite)].drop(columns=['Composite_Key'])
                    
                    if not bmapv_filtered.empty:
                        # Add buttons side by side using columns
                        col1, col2 = st.columns(2)
                        with col1:
                            st.button("Generate Query", key="gen_query_bmapv")
                        with col2:
                            st.button("Execute Query", key="exec_query_bmapv")
                            
                        st.dataframe(bmapv_filtered.dropna(how='all'))
                    else:
                        st.write("No BMAP Values found for the selected BMAP entries.")
                else:
                    # Show empty placeholder or sample structure
                    if not bmapv_df.empty:
                        # Add buttons side by side using columns
                        col1, col2 = st.columns(2)
                        with col1:
                            st.button("Generate Query", key="gen_query_bmapv_empty")
                        with col2:
                            st.button("Execute Query", key="exec_query_bmapv_empty")
                            
                        # Display empty dataframe with column headers to show structure
                        st.dataframe(bmapv_df.head(0))
                    st.write("Select tables above to view related BMAP entries and their values.")
        except Exception as e:
            st.error(f"Error loading BMAP Values: {str(e)}")
# ----------------------------------------------------------------------------------------------------------------------------------
        # CORE Tables
        try:
            if "CORE tables" in sheet_names:
                core_df = load_excel_sheet(file_content, "CORE tables")
                st.subheader("CORE Tables")
                
                # Add buttons side by side using columns
                col1, col2 = st.columns(2)
                with col1:
                    st.button("Generate Query", key="gen_query_core")
                with col2:
                    st.button("Execute Query", key="exec_query_core")
                    
                st.dataframe(core_df.dropna(how='all'))
        except Exception as e:
            st.error(f"Error loading CORE Tables: {str(e)}")
# ----------------------------------------------------------------------------------------------------------------------------------

    with st.expander("Show Full Dataset Preview"):
        sheet_names = st.session_state.sheet_names
        for sheet in sheet_names:
            try:
                df = load_excel_sheet(file_content, sheet)
                st.subheader(f"{sheet} Sheet")
                
                # Add buttons side by side using columns
                col1, col2 = st.columns(2)
                with col1:
                    st.button("Generate Query", key=f"gen_query_preview_{sheet}")
                with col2:
                    st.button("Execute Query", key=f"exec_query_preview_{sheet}")
                    
                st.dataframe(df.head(5).dropna(how='all'))
            except Exception as e:
                st.error(f"Error loading {sheet} sheet: {str(e)}")

    # Export functionality
    st.subheader("Export Data")
    st.write("Export the current filtered data to an Excel file with the same format as the input.")
    
    if st.button("Export to Excel"):
        try:
            # Create a new Excel writer
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Export System sheet
                if 'system_data' in st.session_state and st.session_state.system_data is not None:
                    st.session_state.system_data.to_excel(writer, sheet_name="System", index=False)
                
                # Export STG tables sheet (filtered if a system is selected)
                if 'stg_df' in locals():
                    if selected_tables and 'filtered_data' in locals():
                        # Export only the selected tables data
                        filtered_data.to_excel(writer, sheet_name="STG tables", index=False)
                    elif selected_system:
                        # Export system-filtered data
                        stg_filtered.to_excel(writer, sheet_name="STG tables", index=False)
                    else:
                        # Export all STG data if no filtering
                        stg_df.to_excel(writer, sheet_name="STG tables", index=False)
                
                # Export BKEY sheet (filtered if tables are selected)
                if 'bkey_df' in locals():
                    if 'bkey_filtered' in locals() and not bkey_filtered.empty and selected_tables:
                        bkey_filtered.to_excel(writer, sheet_name="BKEY", index=False)
                    else:
                        bkey_df.to_excel(writer, sheet_name="BKEY", index=False)
                
                # Export BMAP sheet (filtered if tables are selected)
                if 'bmap_df' in locals():
                    if 'bmap_filtered' in locals() and not bmap_filtered.empty and selected_tables:
                        bmap_filtered.to_excel(writer, sheet_name="BMAP", index=False)
                    else:
                        bmap_df.to_excel(writer, sheet_name="BMAP", index=False)
                
                # Export BMAP Values sheet (filtered if BMAP is filtered)
                if 'bmapv_df' in locals():
                    if 'bmapv_filtered' in locals() and not bmapv_filtered.empty:
                        # Make sure to export without the temporary Composite_Key column
                        if 'Composite_Key' in bmapv_filtered.columns:
                            bmapv_filtered = bmapv_filtered.drop(columns=['Composite_Key'])
                        bmapv_filtered.to_excel(writer, sheet_name="BMAP Values", index=False)
                    else:
                        bmapv_df.to_excel(writer, sheet_name="BMAP Values", index=False)
                
                # Export CORE tables sheet (always as is)
                if 'core_df' in locals():
                    core_df.to_excel(writer, sheet_name="CORE tables", index=False)
            
            # Provide download button for the Excel file
            output.seek(0)
            st.download_button(
                label="Download Excel File",
                data=output,
                file_name="filtered_workbook.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            st.success("Excel file created successfully! Click the download button above to save it.")
        except Exception as e:
            st.error(f"Error creating Excel file: {str(e)}")