import streamlit as st     
import pandas as pd
from pymongo import MongoClient
import json
from util.compBetweenJson import jsonDiff
st.title("upload to datbase ")

client = MongoClient("mongodb+srv://omar:test@cluster0.8s1r0a2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["smx"]  

col1,col2 = st.columns([2,2])
with col1:
    xls = st.file_uploader(label="upload file one", type=['xlsx', 'xls'])
with col2:
    xls2 = st.file_uploader(label="upload file two", type=['xlsx', 'xls'])
smx=[xls,xls2]
if xls and xls2 is not None:
    tabs_dict={}
    for i,file in enumerate(smx):
        excel_file = pd.ExcelFile(file)
        st.success(f"File uploaded successfully! Found {len(excel_file.sheet_names)} sheets.")
    
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(file, sheet_name=sheet_name)
        
        
        # st.dataframe(df)
            
            data = df.to_dict(orient='records')
            collection = db[sheet_name]
            print(collection)
            
            if data:
            # collection.insert_many(data)
            # st.write(f"✅ Inserted {len(data)} records from sheet '{sheet_name}' into collection '{sheet_name}'")
            
            # Generate JSON file for this sheet (convert datetime to string)

                json_data = json.loads(df.to_json(orient='records', date_format='iso'))
                tabs_dict[sheet_name]=data
                
                with open(f"pages/jsonFiles/snapshot{i+1}.json", "w") as f:
                    json.dump(json_data, f, indent=4)
                    st.write(f"Generated snapshot{i+1}.json with {len(json_data)} records")
            else:
                st.write(f"Sheet '{sheet_name}' is empty, skipping.")
        tabs_json= json.dump(tabs_dict)

        with open(f"pages/jsonFiles/snapshot_{i}.json", "w") as f:
            json.dump(tabs_json, f, ident=4)
else:
    st.write("Please upload your Excel file (.xlsx or .xls)")
    
print("the dif between the 2 json file is "+jsonDiff)
