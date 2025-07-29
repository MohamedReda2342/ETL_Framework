import streamlit as st     
import pandas as pd
from pymongo import MongoClient

st.title("upload to datbase ")

client = MongoClient("mongodb+srv://omar:test@cluster0.8s1r0a2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["smx"]  

xls = st.file_uploader(label="upload here", type=['xlsx', 'xls'])
if xls is not None:
    # Create ExcelFile object to access sheet names
    excel_file = pd.ExcelFile(xls)
    
    # Display file info
    st.success(f"File uploaded successfully! Found {len(excel_file.sheet_names)} sheets.")
    
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        
        # Display the dataframe
    
        st.dataframe(df)
        
        
        # Convert to dict and upload to MongoDB
        data = df.to_dict(orient='records')
        collection = db[sheet_name]
        if data:
            collection.insert_many(data)
            st.write(f"✅ Inserted {len(data)} records from sheet '{sheet_name}' into collection '{sheet_name}'")
        else:
            st.write(f"⚠️ Sheet '{sheet_name}' is empty, skipping.")

    st.write("Please upload your Excel file (.xlsx or .xls)")

