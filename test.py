# import streamlit as st
# import pandas as pd
# from datetime import datetime

# # --- INITIALIZATION & MOCK DATA ---
# # Using st.session_state to store data persistently across reruns
# def initialize_data():
#     """Sets up the initial mock data for the application."""
#     if 'initialized' not in st.session_state:
#         st.session_state.initialized = True

#         # [cite_start]Simulate data from HR System [cite: 34, 64]
#         st.session_state.employees = {
#             "E001": {"name": "Alice"},
#             "E002": {"name": "Bob"},
#             "E003": {"name": "Charlie"}
#         }

#         # [cite_start]Simulate data from SAP (Billable) and internal creation (Non-Billable) [cite: 35, 66]
#         st.session_state.projects = pd.DataFrame([
#             {"ProjectID": "SAP-001", "Name": "Client A - Website Relaunch", "BillableFlag": True},
#             {"ProjectID": "SAP-002", "Name": "Client B - Mobile App", "BillableFlag": True},
#             {"ProjectID": "INT-001", "Name": "Internal - Q4 Training", "BillableFlag": False},
#             {"ProjectID": "INT-002", "Name": "Internal - Admin", "BillableFlag": False}
#         ])

#         # [cite_start]Initialize an empty DataFrame for timesheet entries [cite: 68]
#         st.session_state.timesheets = pd.DataFrame(columns=[
#             "EntryID", "EmployeeID", "ProjectID", "Date", "Hours", "Notes", "Status"
#         ])
#         st.session_state.entry_counter = 0

# # Call initialization function
# initialize_data()


# # --- HELPER FUNCTIONS ---
# def get_project_name(project_id):
#     """Fetches project name from the projects DataFrame."""
#     return st.session_state.projects.loc[st.session_state.projects['ProjectID'] == project_id, 'Name'].iloc[0]

# # --- UI SECTIONS ---

# # --- EMPLOYEE VIEW ---
# def employee_view(user_id):
#     st.header(f"Welcome, {st.session_state.employees[user_id]['name']}")
#     st.subheader("📝 Enter Your Timesheet")

#     # [cite_start]Timesheet entry form [cite: 48]
#     with st.form("timesheet_form"):
#         project_id = st.selectbox(
#             "Select Project",
#             options=st.session_state.projects['ProjectID'],
#             format_func=lambda x: f"{x} - {get_project_name(x)}"
#         )
#         entry_date = st.date_input("Date")
#         hours = st.number_input("Hours Worked", min_value=0.5, max_value=24.0, step=0.5)
#         notes = st.text_area("Notes / Task Description")

#         submitted = st.form_submit_button("Submit Timesheet")

#         if submitted:
#             # Add new entry to the timesheets DataFrame
#             st.session_state.entry_counter += 1
#             new_entry = pd.DataFrame([{
#                 "EntryID": st.session_state.entry_counter,
#                 "EmployeeID": user_id,
#                 "ProjectID": project_id,
#                 "Date": entry_date,
#                 "Hours": hours,
#                 "Notes": notes,
#                 "Status": "Submitted"  # Default status on creation
#             }])
#             st.session_state.timesheets = pd.concat([st.session_state.timesheets, new_entry], ignore_index=True)
#             st.success("Timesheet submitted successfully!")

#     st.divider()
#     st.subheader("📄 My Timesheet History")
#     my_timesheets = st.session_state.timesheets[st.session_state.timesheets['EmployeeID'] == user_id]
#     st.dataframe(my_timesheets, use_container_width=True)


# # --- PROJECT MANAGER VIEW ---
# def project_manager_view():
#     st.header("Project Manager Dashboard")
#     st.subheader("Approval Queue")

#     pending_sheets = st.session_state.timesheets[st.session_state.timesheets['Status'] == 'Submitted']

#     if pending_sheets.empty:
#         st.info("No timesheets are pending approval.")
#     else:
#         for index, row in pending_sheets.iterrows():
#             st.markdown(f"**Entry ID:** {row['EntryID']} | **Employee:** {st.session_state.employees[row['EmployeeID']]['name']} | **Project:** {row['ProjectID']}")
#             st.markdown(f"**Date:** {row['Date']} | **Hours:** {row['Hours']}")
#             with st.expander("View Details and Notes"):
#                 st.write(row['Notes'])

#             # Create columns for buttons
#             col1, col2 = st.columns(2)
#             with col1:
#                 if st.button("Approve", key=f"approve-{row['EntryID']}"):
#                     st.session_state.timesheets.loc[st.session_state.timesheets['EntryID'] == row['EntryID'], 'Status'] = 'Approved'
#                     st.success(f"Entry {row['EntryID']} approved.")
#                     st.rerun()
#             with col2:
#                 if st.button("Reject", key=f"reject-{row['EntryID']}"):
#                     st.session_state.timesheets.loc[st.session_state.timesheets['EntryID'] == row['EntryID'], 'Status'] = 'Rejected'
#                     st.warning(f"Entry {row['EntryID']} rejected.")
#                     st.rerun()
#             st.divider()


# # --- HR / FINANCE VIEW ---
# def hr_finance_view():
#     st.header("HR & Finance Dashboard")
#     st.subheader("Approved Timesheets for Locking")

#     approved_sheets = st.session_state.timesheets[st.session_state.timesheets['Status'] == 'Approved']

#     if not approved_sheets.empty:
#         st.data_editor(
#             approved_sheets,
#             column_config={
#                 "Lock": st.column_config.CheckboxColumn(
#                     "Lock for Payroll?",
#                     default=False,
#                 )
#             },
#             key="lock_editor"
#         )
#         if st.button("Confirm Lock"):
#             edited_rows = st.session_state["lock_editor"]["edited_rows"]
#             for index, change in edited_rows.items():
#                 if change.get("Lock"): # If the checkbox was ticked
#                     original_entry_id = approved_sheets.iloc[index]['EntryID']
#                     st.session_state.timesheets.loc[st.session_state.timesheets['EntryID'] == original_entry_id, 'Status'] = 'Locked'
#             st.success("Selected timesheets have been locked.")
#             st.rerun()
#     else:
#         st.info("No approved timesheets are ready for locking.")


#     # [cite_start]--- Reporting Section [cite: 116] ---
#     st.divider()
#     st.subheader("📊 Reports")
#     if not st.session_state.timesheets.empty:
#         # Merge with projects to get BillableFlag
#         report_df = pd.merge(st.session_state.timesheets, st.session_state.projects, on="ProjectID")

#         # Billable vs. Non-billable Pie Chart
#         utilization = report_df.groupby('BillableFlag')['Hours'].sum()
#         utilization.index = utilization.index.map({True: 'Billable', False: 'Non-Billable'})
#         st.write("#### Billable vs. Non-Billable Hours")
#         st.bar_chart(utilization)
#     else:
#         st.write("No timesheet data available for reporting.")


# # --- ADMIN VIEW ---
# def admin_view():
#     st.header("System Administration")
#     st.subheader("Manage Non-Billable Projects")

#     with st.form("new_project_form"):
#         new_project_id = st.text_input("New Project ID (e.g., INT-003)")
#         new_project_name = st.text_input("New Project Name")
#         submitted = st.form_submit_button("Create Project")

#         if submitted and new_project_id and new_project_name:
#             new_project = pd.DataFrame([{
#                 "ProjectID": new_project_id,
#                 "Name": new_project_name,
#                 "BillableFlag": False
#             }])
#             st.session_state.projects = pd.concat([st.session_state.projects, new_project], ignore_index=True)
#             st.success(f"Project '{new_project_name}' created successfully.")
#         elif submitted:
#             st.error("Please provide both a Project ID and a Name.")

#     st.divider()
#     st.subheader("Current Project List")
#     st.dataframe(st.session_state.projects, use_container_width=True)


# # --- MAIN APP LAYOUT ---
# st.title("🕒 Timesheet Approval Application")

# # [cite_start]Sidebar for role selection [cite: 42]
# st.sidebar.title("Navigation")
# role = st.sidebar.selectbox("Select Your Role", ["Employee", "Project Manager", "HR/Finance", "Admin"])

# # Display view based on role
# if role == "Employee":
#     # Let user select their employee ID
#     user_id = st.sidebar.selectbox("Select Your User ID", options=list(st.session_state.employees.keys()), format_func=lambda x: f"{x} - {st.session_state.employees[x]['name']}")
#     employee_view(user_id)
# elif role == "Project Manager":
#     project_manager_view()
# elif role == "HR/Finance":
#     hr_finance_view()
# elif role == "Admin":
#     admin_view()