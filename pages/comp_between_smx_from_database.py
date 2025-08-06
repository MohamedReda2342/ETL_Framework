
import streamlit as st     
import pandas as pd
from pymongo import MongoClient
import json

st.title("compare between smx versions")
client = MongoClient("mongodb+srv://omar:test@cluster0.8s1r0a2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["smx"]
if 'sleceted_versions' and 'all_data' not in st.session_state: 
    st.session_state.sleceted_versions=[]
    st.session_state.all_data = {}
smx_versions=db.list_collection_names()

choice = st.selectbox(label="slecet the the smx version",options=smx_versions)

if st.button(label="save option"):
    if choice not in st.session_state.sleceted_versions :
        st.session_state.sleceted_versions.append(choice)
        st.write(st.session_state.sleceted_versions)
    else :
        st.error("you can't select the same version more than one time ❌")

col1,col2 = st.columns(2)    
col = [col1,col2]

for name in st.session_state.sleceted_versions :
    # name is the name of the collection
    current_collection = db[name]
    document = list(current_collection.find()) # hold the data from the collection as a list
    
    st.session_state.all_data[name]=document # store the data in a object with the name of the collection as key
    
    st.subheader(f"SMX Version: {name}")
    
    try:
        for doc in st.session_state.all_data[name]:
            sheet_name = doc.get("sheet name", "Unknown Sheet")  # Get the sheet name
            records = doc.get("records", [])  # Get the list of records

            if records:  # Only display if there are records
                st.write(f"### {sheet_name}")
                df = pd.DataFrame(records)
                st.dataframe(df, use_container_width=True)
            else:
                st.write(f"### {sheet_name}")
                st.info("No records found in this sheet")

    except Exception as e:
        st.error(f"Could not display as dataframe: {e}")
        st.write("Raw data:")
        st.write(st.session_state.all_data[name]) 

        
