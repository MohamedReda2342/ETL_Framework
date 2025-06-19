import streamlit as st
import pandas as pd
from io import BytesIO
import difflib

# Set page config
st.set_page_config(page_title="WAVZ SMX Workbook Comparator", layout="wide")

# Title
st.title("WAVZ SMX Workbook Comparator")

# Function to load and process workbook
def load_workbook(uploaded_file):
    if uploaded_file is None:
        return None
    
    # Read all sheets from the Excel file
    xls = pd.ExcelFile(uploaded_file)
    sheets = {}
    
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
        # Clean up the dataframe (remove empty rows/columns)
        df = df.dropna(how='all').dropna(axis=1, how='all')
        sheets[sheet_name] = df
    
    return sheets

# Function to compare two dataframes
def compare_dataframes(df1, df2, sheet_name):
    if df1 is None or df2 is None:
        return None
    
    # Convert dataframes to strings line by line
    df1_lines = df1.to_string(index=False).split('\n')
    df2_lines = df2.to_string(index=False).split('\n')
    
    # Generate diff
    diff = difflib.unified_diff(
        df1_lines, 
        df2_lines,
        fromfile=f"Workbook 1 - {sheet_name}",
        tofile=f"Workbook 2 - {sheet_name}",
        lineterm=""
    )
    
    return '\n'.join(diff)

# Function to highlight differences between two dataframes
def highlight_diff(df1, df2):
    if df1 is None or df2 is None:
        return None
    
    # Ensure dataframes have same shape and columns
    if not df1.shape == df2.shape or not all(df1.columns == df2.columns):
        return pd.DataFrame(["Dataframes have different structures. Cannot compare cell by cell."])
    
    # Create a styled dataframe that highlights differences
    comparison_df = df1.copy()
    for col in df1.columns:
        for idx in df1.index:
            val1 = str(df1.at[idx, col])
            val2 = str(df2.at[idx, col])
            if val1 != val2:
                comparison_df.at[idx, col] = f"{val1} → {val2}"
    
    return comparison_df

# Upload files
col1, col2 = st.columns(2)
with col1:
    st.subheader("Workbook 1")
    workbook1 = st.file_uploader("Upload first workbook", type=["xlsx"], key="wb1")
    
with col2:
    st.subheader("Workbook 2")
    workbook2 = st.file_uploader("Upload second workbook", type=["xlsx"], key="wb2")

# Load workbooks if files are uploaded
wb1_sheets = load_workbook(workbook1)
wb2_sheets = load_workbook(workbook2)

if wb1_sheets and wb2_sheets:
    st.success("Both workbooks loaded successfully!")
    
    # Get common sheets
    common_sheets = set(wb1_sheets.keys()) & set(wb2_sheets.keys())
    different_sheets = set(wb1_sheets.keys()).symmetric_difference(set(wb2_sheets.keys()))
    
    # Show sheet differences
    if different_sheets:
        st.warning("Workbooks have different sheets:")
        st.write(f"Only in Workbook 1: {set(wb1_sheets.keys()) - set(wb2_sheets.keys())}")
        st.write(f"Only in Workbook 2: {set(wb2_sheets.keys()) - set(wb1_sheets.keys())}")
    
    # Select sheet to compare
    selected_sheet = st.selectbox("Select sheet to compare", sorted(common_sheets))
    
    # Display comparison
    if selected_sheet:
        df1 = wb1_sheets[selected_sheet]
        df2 = wb2_sheets[selected_sheet]
        
        # Show side by side view
        st.subheader(f"Comparison of: {selected_sheet}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("Workbook 1")
            st.dataframe(df1)
        
        with col2:
            st.write("Workbook 2")
            st.dataframe(df2)
        
        # Show differences
        st.subheader("Differences Highlighted")
        diff_df = highlight_diff(df1, df2)
        st.dataframe(diff_df)
        
        # Show unified diff
        st.subheader("Detailed Differences (Unified Diff Format)")
        diff_text = compare_dataframes(df1, df2, selected_sheet)
        st.text(diff_text)
        
        # Option to download the comparison
        st.download_button(
            label="Download comparison as text",
            data=diff_text,
            file_name=f"comparison_{selected_sheet}.txt",
            mime="text/plain"
        )
        
elif wb1_sheets or wb2_sheets:
    st.warning("Please upload both workbooks to compare")
else:
    st.info("Upload two WAVZ SMX workbooks to compare them")