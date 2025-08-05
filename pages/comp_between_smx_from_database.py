
import streamlit as st     
import pandas as pd
from pymongo import MongoClient
import json

st.title("upload to datbase ")

client = MongoClient("mongodb+srv://omar:test@cluster0.8s1r0a2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["smx"]  

st.write(db.list_collection_names())