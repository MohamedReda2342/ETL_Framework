import streamlit as st

options = ['Apple', 'Banana', 'Orange', 'Grape']

# Create the checkbox for "Select All"
select_all = st.checkbox("Select All")

if select_all:
    # If "Select All" is checked, pre-select all options in the multiselect
    selected_options = st.multiselect("Select fruits:", options, default=options)
else:
    # Otherwise, allow the user to select options manually
    selected_options = st.multiselect("Select fruits:", options)

st.write("You selected:", selected_options)