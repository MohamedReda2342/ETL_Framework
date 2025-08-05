import streamlit as st     
import pandas as pd
from pymongo import MongoClient
import json

st.title("upload to datbase ")

client = MongoClient("mongodb+srv://omar:test@cluster0.8s1r0a2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["smx"]  

xls = st.file_uploader(label="upload file one", type=['xlsx', 'xls'])
if xls is not None:
    excel_file = pd.ExcelFile(xls)
    file_name = xls.name
    file_name_without_ext = file_name.rsplit('.',1)[0]
    
    collection = db[file_name_without_ext]
    st.success(f"File uploaded successfully! Found {len(excel_file.sheet_names)} sheets.")
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        st.write(file_name_without_ext)
        # st.dataframe(df)
    
        data = df.to_dict(orient='records')
        if data:
            sheet_document = {
                "sheet name":sheet_name,
                "records":data
            }
            collection.insert_one(sheet_document)
                # st.write(f"✅ Inserted {len(data)} records from sheet '{sheet_name}' into collection '{sheet_name}'")
            

#endregion






#region Commented Upload Code