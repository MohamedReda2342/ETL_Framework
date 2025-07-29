import streamlit as st
import yaml
from yaml.loader import SafeLoader
import os
from util.auth import check_authentication

# Authentication check - must be first command
authenticator = check_authentication()

# Check if user has admin role
if "roles" not in st.session_state or "admin" not in st.session_state["roles"]:
    st.error("You don't have permission to access this page")
    st.stop()
authenticator.logout("Logout", "sidebar")


# Load config functions
def load_config():
    with open("config.yaml") as file:
        return yaml.load(file, Loader=SafeLoader)


def save_config(config):
    with open("config.yaml", "w") as file:
        yaml.dump(config, file, default_flow_style=False)


# Initialize session state for edit mode
if "edit_user" not in st.session_state:
    st.session_state.edit_user = None

# Load current configuration
config = load_config()
users = config["credentials"]["usernames"]
roles = config["roles"]
privileges = config["privileges"]


# Dialog for editing user
@st.dialog("Edit User")
def edit_user_dialog(username):
    user_data = users[username]

    st.write(f"**Editing user: {username}**")

    with st.form(f"edit_user_form_{username}"):
        col1, col2 = st.columns(2)

        with col1:
            edit_first_name = st.text_input("First Name", value=user_data["first_name"])
            edit_password = st.text_input("New Password ", type="password")

        with col2:
            edit_last_name = st.text_input("Last Name", value=user_data["last_name"])
            edit_roles = st.multiselect(
                "Roles", ["user", "admin"], default=user_data["roles"]
            )

        col1_btn, col2_btn = st.columns(2)
        with col1_btn:
            update_submitted = st.form_submit_button("Update User", type="primary")
        with col2_btn:
            cancel_submitted = st.form_submit_button("Cancel")

        if update_submitted:
            if edit_first_name and edit_last_name and edit_roles:
                users[username]["first_name"] = edit_first_name
                users[username]["last_name"] = edit_last_name
                users[username]["roles"] = edit_roles

                if edit_password:
                    users[username]["password"] = edit_password

                save_config(config)
                st.success(f"✅ User '{username}' updated successfully!")
                st.session_state.edit_user = None
                st.rerun()
            else:
                st.error("❗ Please fill in all required fields.")

        if cancel_submitted:
            st.session_state.edit_user = None
            st.rerun()


# Dialog for deleting user
@st.dialog("Delete User")
def delete_user_dialog(username):
    user_data = users[username]
    current_user = st.session_state["username"]

    # Prevent deleting current user
    if username.lower() == current_user:
        st.error("❌ You cannot delete your own account!")
        if st.button("Close"):
            st.rerun()
        return

    st.warning(f"**Are you sure you want to delete user: {username}?**")

    # Confirmation checkbox
    confirm_delete = st.checkbox(f"I confirm I want to delete user '{username}'")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Delete User", type="primary", disabled=not confirm_delete):
            # Delete user from config
            del users[username]

            # Save updated config
            save_config(config)
            st.success(f"User '{username}' deleted successfully!")
            st.rerun()

    with col2:
        if st.button("Cancel"):
            st.rerun()


# Tabs for different operations
tab1, tab2, tab3 = st.tabs(["Manage Users", "Manage Roles", "Manage Privileges"])


# Dialog for adding new user
@st.dialog("Add New User")
def add_user_dialog():
    st.write("**Add New User**")

    with st.form("add_user_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_username = st.text_input("Username")
            new_first_name = st.text_input("First Name")
        with col2:
            new_last_name = st.text_input("Last Name")
            new_password = st.text_input("Password", type="password")
        new_roles = st.multiselect("Roles", ["user", "admin"], default=["user"])

        col1_btn, col2_btn = st.columns(2)
        with col1_btn:
            add_submitted = st.form_submit_button("Add User", type="primary")
        with col2_btn:
            cancel_submitted = st.form_submit_button("Cancel")

        if add_submitted:
            if (
                new_username
                and new_first_name
                and new_last_name
                and new_password
                and new_roles
            ):
                if new_username not in users:
                    # Add new user to config
                    users[new_username] = {
                        "failed_login_attempts": 0,
                        "first_name": new_first_name,
                        "last_name": new_last_name,
                        "logged_in": False,
                        "password": new_password,  # This will be hashed on first run
                        "roles": new_roles,
                    }

                    # Save updated config
                    save_config(config)
                    st.success(f"✅ User '{new_username}' added successfully!")
                    st.rerun()
                else:
                    st.error("❗ Username already exists!")
            else:
                st.error("❗ Please fill in all required fields.")

        if cancel_submitted:
            st.rerun()


# Replace the tab1 section with this updated code:

with tab1:
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Add New User", type="primary"):
            add_user_dialog()

    cols = st.columns(4)
    fields = ["Username", "Role", "Edit", "Delete"]

    # Header
    for col, field in zip(cols, fields):
        col.write("**" + field + "**")

    # Rows
    for username, user_data in users.items():
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.write(username)

        with col2:
            st.write(f"{', '.join(user_data['roles'])}")

        with col3:
            if st.button("Edit", key=f"edit_button_{username}", type="primary"):
                edit_user_dialog(username)

        with col4:
            current_user = st.session_state.get("username")
            is_current_user = username == current_user

            # Disable delete button for current user and show help text
            help_text = "Cannot delete your own account" if is_current_user else None

            if st.button(
                "Delete",
                key=f"delete_button_{username}",
                disabled=is_current_user,
                help=help_text,
            ):
                delete_user_dialog(username)


# Dialog for editing role
@st.dialog("Edit Role")
def edit_role_dialog(role_name):
    role_data = roles[role_name]

    st.write(f"**Editing role: {role_name}**")

    with st.form(f"edit_role_form_{role_name}"):
        edit_description = st.text_area(
            "Description", value=role_data.get("description", "")
        )
        edit_privileges = st.multiselect(
            "Privileges", privileges, default=role_data.get("privileges", [])
        )

        col1_btn, col2_btn = st.columns(2)
        with col1_btn:
            update_submitted = st.form_submit_button("Update Role", type="primary")
        with col2_btn:
            cancel_submitted = st.form_submit_button("Cancel")

        if update_submitted:
            if edit_description and edit_privileges:
                roles[role_name]["description"] = edit_description
                roles[role_name]["privileges"] = edit_privileges

                save_config(config)
                st.success(f"✅ Role '{role_name}' updated successfully!")
                st.rerun()
            else:
                st.error("❗ Please fill in all required fields.")

        if cancel_submitted:
            st.rerun()


# Dialog for deleting role
@st.dialog("Delete Role")
def delete_role_dialog(role_name):
    role_data = roles[role_name]

    st.warning(f"**Are you sure you want to delete role: {role_name}?**")

    st.write("**Role Details:**")
    st.write(f"Description: {role_data.get('description', 'No description')}")
    st.write(f"Privileges: {', '.join(role_data.get('privileges', []))}")

    # Check if role is in use by any users
    users_with_role = [
        username
        for username, user_data in users.items()
        if role_name in user_data.get("roles", [])
    ]

    if users_with_role:
        st.error(
            f"❌ Cannot delete role '{role_name}' because it's assigned to the following users:"
        )
        for user in users_with_role:
            st.write(f"- {user}")
        st.info("Please remove this role from all users before deleting it.")

        if st.button("Cancel"):
            st.rerun()
        return

    st.error("⚠️ This action cannot be undone!")

    # Confirmation checkbox
    confirm_delete = st.checkbox(f"I confirm I want to delete role '{role_name}'")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Delete Role", type="primary", disabled=not confirm_delete):
            # Delete role from config
            del roles[role_name]

            # Save updated config
            save_config(config)
            st.success(f"Role '{role_name}' deleted successfully!")
            st.rerun()

    with col2:
        if st.button("Cancel"):
            st.rerun()


# Dialog for adding new role
@st.dialog("Add New Role")
def add_role_dialog():
    st.write("**Create New Role**")

    with st.form("add_role_form"):
        new_role_name = st.text_input("Role Name")
        new_role_description = st.text_area("Description")
        new_role_privileges = st.multiselect("Privileges", privileges)

        col1_btn, col2_btn = st.columns(2)
        with col1_btn:
            add_submitted = st.form_submit_button("Add Role", type="primary")
        with col2_btn:
            cancel_submitted = st.form_submit_button("Cancel")

        if add_submitted:
            if new_role_name and new_role_description and new_role_privileges:
                if new_role_name not in roles:
                    # Add new role to config
                    roles[new_role_name] = {
                        "description": new_role_description,
                        "privileges": new_role_privileges,
                    }

                    save_config(config)
                    st.success(f"✅ Role '{new_role_name}' added successfully!")
                    st.rerun()
                else:
                    st.error("❗ Role name already exists!")
            else:
                st.error("❗ Please fill in all required fields.")

        if cancel_submitted:
            st.rerun()


# Updated tab2 content
with tab2:
    # Add new role button
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Add New Role", type="primary"):
            add_role_dialog()

    # Table header
    cols = st.columns([2, 3, 1, 1])
    fields = ["Role", "Privileges", "Edit", "Delete"]

    for col, field in zip(cols, fields):
        col.write("**" + field + "**")

    # Table rows
    for role_name, role_data in roles.items():
        col1, col2, col3, col4 = st.columns([2, 3, 1, 1])

        with col1:
            st.write(role_name)

        with col2:
            privileges_text = ", ".join(role_data.get("privileges", []))
            # Truncate long privilege lists
            if len(privileges_text) > 50:
                privileges_text = privileges_text[:50] + "..."
            st.write(privileges_text)

        with col3:
            if st.button("Edit", key=f"edit_role_button_{role_name}", type="primary"):
                edit_role_dialog(role_name)

        with col4:
            # Prevent deletion of default roles if needed
            disabled = role_name in ["admin", "user"]  # Protect default roles
            help_text = "Cannot delete default roles" if disabled else None

            if st.button(
                "Delete",
                key=f"delete_role_button_{role_name}",
                disabled=disabled,
                help=help_text,
            ):
                delete_role_dialog(role_name)

                # Dialog for adding new privilege


@st.dialog("Add New Privilege")
def add_privilege_dialog():
    st.write("**Add New Privilege**")

    with st.form("add_privilege_form"):
        new_privilege = st.text_input("Privilege Name")

        col1_btn, col2_btn = st.columns(2)
        with col1_btn:
            add_submitted = st.form_submit_button("Add Privilege", type="primary")
        with col2_btn:
            cancel_submitted = st.form_submit_button("Cancel")

        if add_submitted:
            if new_privilege:
                if new_privilege not in privileges:
                    # Add new privilege to config
                    privileges.append(new_privilege)

                    save_config(config)
                    st.success(f"✅ Privilege '{new_privilege}' added successfully!")
                    st.rerun()
                else:
                    st.error("❗ Privilege already exists!")
            else:
                st.error("❗ Please enter a privilege name.")

        if cancel_submitted:
            st.rerun()


# Dialog for editing privilege
@st.dialog("Edit Privilege")
def edit_privilege_dialog(old_privilege):
    with st.form(f"edit_privilege_form_{old_privilege}"):
        new_privilege_name = st.text_input("Privilege Name", value=old_privilege)

        col1_btn, col2_btn = st.columns(2)
        with col1_btn:
            update_submitted = st.form_submit_button("Update Privilege", type="primary")
        with col2_btn:
            cancel_submitted = st.form_submit_button("Cancel")

        if update_submitted:
            if new_privilege_name:
                if (
                    new_privilege_name != old_privilege
                    and new_privilege_name in privileges
                ):
                    st.error("❗ Privilege name already exists!")
                elif new_privilege_name != old_privilege:
                    # Update privilege name in the privileges list
                    privilege_index = privileges.index(old_privilege)
                    privileges[privilege_index] = new_privilege_name

                    # Update privilege name in all roles that use it
                    for role_name, role_data in roles.items():
                        if old_privilege in role_data.get("privileges", []):
                            role_privileges = role_data["privileges"]
                            role_privileges[role_privileges.index(old_privilege)] = (
                                new_privilege_name
                            )

                    save_config(config)
                    st.success(
                        f"✅ Privilege updated from '{old_privilege}' to '{new_privilege_name}' successfully!"
                    )
                    st.rerun()
                else:
                    st.info("No changes made.")
                    st.rerun()
            else:
                st.error("❗ Please enter a privilege name.")

        if cancel_submitted:
            st.rerun()


# Dialog for deleting privilege
@st.dialog("Delete Privilege")
def delete_privilege_dialog(privilege_name):
    st.warning(f"**Are you sure you want to delete privilege: {privilege_name}?**")

    # Check if privilege is in use by any roles
    roles_with_privilege = [
        role_name
        for role_name, role_data in roles.items()
        if privilege_name in role_data.get("privileges", [])
    ]

    if roles_with_privilege:
        st.error(
            f"❌ Cannot delete privilege '{privilege_name}' because it's assigned to the following roles:"
        )
        for role in roles_with_privilege:
            st.write(f"- {role}")
        st.info("Please remove this privilege from all roles before deleting it.")

        if st.button("Cancel"):
            st.rerun()
        return

    st.error("⚠️ This action cannot be undone!")

    # Confirmation checkbox
    confirm_delete = st.checkbox(
        f"I confirm I want to delete privilege '{privilege_name}'"
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Delete Privilege", type="primary", disabled=not confirm_delete):
            # Delete privilege from config
            privileges.remove(privilege_name)

            # Save updated config
            save_config(config)
            st.success(f"Privilege '{privilege_name}' deleted successfully!")
            st.rerun()

    with col2:
        if st.button("Cancel"):
            st.rerun()


# Tab3 content for managing privileges
with tab3:
    # Add new privilege button
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Add New Privilege", type="primary"):
            add_privilege_dialog()

    # Table header
    cols = st.columns([3, 1, 1])
    fields = ["Privilege", "Edit", "Delete"]

    for col, field in zip(cols, fields):
        col.write("**" + field + "**")

    # Table rows
    for privilege in privileges:
        col1, col2, col3 = st.columns([3, 1, 1])

        with col1:
            st.write(privilege)

        with col2:
            if st.button(
                "Edit", key=f"edit_privilege_button_{privilege}", type="primary"
            ):
                edit_privilege_dialog(privilege)

        with col3:
            if st.button(
                "Delete", key=f"delete_privilege_button_{privilege}", help=help_text
            ):
                delete_privilege_dialog(privilege)
