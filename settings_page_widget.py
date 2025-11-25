# settings_page_widget.py
import sys
import yaml
import bcrypt
from PySide6.QtCore import Slot, Qt
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTableWidget,
    QTableWidgetItem, QAbstractItemView, QHeaderView, QDialog, QLineEdit,
    QMessageBox, QFormLayout, QDialogButtonBox, QComboBox, QListWidget, QListWidgetItem
)

# --- Helper functions for config file ---
# We assume config.yaml is in the same directory
CONFIG_PATH = 'config.yaml'

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        QMessageBox.critical(None, "Config Error", f"Failed to load config.yaml: {e}")
        return None

def save_config(config_data):
    try:
        with open(CONFIG_PATH, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
        return True
    except Exception as e:
        QMessageBox.critical(None, "Config Error", f"Failed to save config.yaml: {e}")
        return False

# --- Dialog for Adding a New User ---
class AddUserDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add New User")
        
        self.layout = QVBoxLayout(self)
        form_layout = QFormLayout()
        
        self.username = QLineEdit()
        self.first_name = QLineEdit()
        self.last_name = QLineEdit()
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.EchoMode.Password)
        
        # Simple ComboBox for roles for this example
        self.roles = QComboBox()
        self.roles.addItems(["user", "admin"])
        
        form_layout.addRow("Username:", self.username)
        form_layout.addRow("First Name:", self.first_name)
        form_layout.addRow("Last Name:", self.last_name)
        form_layout.addRow("Password:", self.password)
        form_layout.addRow("Role:", self.roles)
        
        self.layout.addLayout(form_layout)
        
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        
        self.layout.addWidget(buttons)

    def get_data(self):
        return {
            "username": self.username.text(),
            "first_name": self.first_name.text(),
            "last_name": self.last_name.text(),
            "password": self.password.text(),
            "roles": [self.roles.currentText()] # Simplified to one role
        }

# --- Dialog for Editing a User ---
class EditUserDialog(QDialog):
    def __init__(self, username, user_data, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Edit User: {username}")
        
        self.layout = QVBoxLayout(self)
        form_layout = QFormLayout()
        
        self.first_name = QLineEdit(user_data['first_name'])
        self.last_name = QLineEdit(user_data['last_name'])
        self.password = QLineEdit()
        self.password.setPlaceholderText("Leave blank to keep current password")
        self.password.setEchoMode(QLineEdit.EchoMode.Password)
        
        self.roles = QComboBox()
        self.roles.addItems(["user", "admin"])
        # Set current role
        current_role = user_data.get('roles', ['user'])[0]
        self.roles.setCurrentText(current_role)
        
        form_layout.addRow("First Name:", self.first_name)
        form_layout.addRow("Last Name:", self.last_name)
        form_layout.addRow("New Password:", self.password)
        form_layout.addRow("Role:", self.roles)
        
        self.layout.addLayout(form_layout)
        
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        
        self.layout.addWidget(buttons)

    def get_data(self):
        return {
            "first_name": self.first_name.text(),
            "last_name": self.last_name.text(),
            "password": self.password.text(), # Will be empty if not changed
            "roles": [self.roles.currentText()]
        }

# --- Main Settings Page Widget ---
class SettingsPageWidget(QWidget):
    def __init__(self, authenticator, parent=None):
        super().__init__(parent)
        self.authenticator = authenticator
        self.config = {}
        
        # Main layout for this widget
        self.main_layout = QVBoxLayout(self)
        


    def check_permissions(self):
        # Load config to check roles
        self.config = load_config()
        if not self.config:
            self.main_layout.addWidget(QLabel("Error: Could not load config file."))
            return
            
        try:
            user_data = self.config['credentials']['usernames'][self.authenticator.username]
            self.current_roles = user_data.get('roles', [])
        except KeyError:
            self.main_layout.addWidget(QLabel("Error: Could not find your user in config."))
            return

        # --- Authorization Check ---
        if 'admin' not in self.current_roles:
            self.main_layout.addWidget(QLabel("<h2>Access Denied</h2>You do not have permission to view this page."))
        else:
            self._init_admin_ui()

    def _init_admin_ui(self):
        # User is an admin, build the UI
        self.tabs = QTabWidget()
        self.main_layout.addWidget(self.tabs)
        
        # --- Create Tabs ---
        self.users_tab = self._create_users_tab()
        self.roles_tab = QWidget() # Stub
        self.privileges_tab = QWidget() # Stub
        
        self.tabs.addTab(self.users_tab, "Manage Users")
        self.tabs.addTab(self.roles_tab, "Manage Roles")
        self.tabs.addTab(self.privileges_tab, "Manage Privileges")
        
        # Add stub content to other tabs
        self.roles_tab.setLayout(QVBoxLayout())
        self.roles_tab.layout().addWidget(QLabel("Role management can be built here, following the same pattern as the Users tab."))
        
        self.privileges_tab.setLayout(QVBoxLayout())
        self.privileges_tab.layout().addWidget(QLabel("Privilege management can be built here, following the same pattern as the Users tab."))

        # Load initial data
        self._populate_users_table()

    # --- Manage Users Tab ---
    def _create_users_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        btn_layout = QHBoxLayout()
        self.btn_add_user = QPushButton("Add New User")
        self.btn_add_user.clicked.connect(self._on_add_user)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_add_user)
        layout.addLayout(btn_layout)
        
        self.users_table = QTableWidget()
        self.users_table.setColumnCount(4)
        self.users_table.setHorizontalHeaderLabels(["Username", "Role(s)", "Edit", "Delete"])
        self.users_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.users_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.users_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        layout.addWidget(self.users_table)
        return widget

    def _populate_users_table(self):
        """This function acts like st.rerun for the user table"""
        self.config = load_config() # Re-load config
        users = self.config['credentials']['usernames']
        
        self.users_table.setRowCount(0) # Clear table
        
        for username, data in users.items():
            row_position = self.users_table.rowCount()
            self.users_table.insertRow(row_position)
            
            self.users_table.setItem(row_position, 0, QTableWidgetItem(username))
            self.users_table.setItem(row_position, 1, QTableWidgetItem(", ".join(data.get('roles', []))))
            
            # --- Edit Button ---
            btn_edit = QPushButton("Edit")
            btn_edit.clicked.connect(lambda checked, u=username: self._on_edit_user(u))
            self.users_table.setCellWidget(row_position, 2, btn_edit)

            # --- Delete Button ---
            btn_delete = QPushButton("Delete")
            btn_delete.clicked.connect(lambda checked, u=username: self._on_delete_user(u))
            
            # Prevent self-deletion
            if username == self.authenticator.username:
                btn_delete.setDisabled(True)
                btn_delete.setToolTip("Cannot delete your own account")
                
            self.users_table.setCellWidget(row_position, 3, btn_delete)

    @Slot()
    def _on_add_user(self):
        dialog = AddUserDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            data = dialog.get_data()
            
            # --- Validation ---
            if not all([data['username'], data['first_name'], data['last_name'], data['password'], data['roles']]):
                QMessageBox.warning(self, "Error", "Please fill in all required fields.")
                return
            
            if data['username'] in self.config['credentials']['usernames']:
                QMessageBox.warning(self, "Error", "Username already exists!")
                return
                
            # --- Add User ---
            hashed_password = bcrypt.hashpw(data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            self.config['credentials']['usernames'][data['username']] = {
                'failed_login_attempts': 0,
                'first_name': data['first_name'],
                'last_name': data['last_name'],
                'logged_in': False,
                'password': hashed_password,
                'roles': data['roles']
            }
            
            if save_config(self.config):
                QMessageBox.information(self, "Success", f"User '{data['username']}' added successfully!")
                self._populate_users_table() # Refresh the table

    @Slot(str)
    def _on_edit_user(self, username):
        user_data = self.config['credentials']['usernames'][username]
        dialog = EditUserDialog(username, user_data, self)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            data = dialog.get_data()
            
            if not all([data['first_name'], data['last_name'], data['roles']]):
                QMessageBox.warning(self, "Error", "First Name, Last Name, and Roles are required.")
                return

            # --- Update User ---
            users = self.config['credentials']['usernames']
            users[username]['first_name'] = data['first_name']
            users[username]['last_name'] = data['last_name']
            users[username]['roles'] = data['roles']
            
            if data['password']: # Only update password if a new one was entered
                hashed_password = bcrypt.hashpw(data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                users[username]['password'] = hashed_password
            
            if save_config(self.config):
                QMessageBox.information(self, "Success", f"User '{username}' updated successfully!")
                self._populate_users_table() # Refresh

    @Slot(str)
    def _on_delete_user(self, username):
        if username == self.authenticator.username:
            QMessageBox.critical(self, "Error", "You cannot delete your own account.")
            return

        reply = QMessageBox.warning(
            self, 
            "Delete User", 
            f"Are you sure you want to delete user: {username}?\nThis action cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # --- Delete User ---
            del self.config['credentials']['usernames'][username]
            if save_config(self.config):
                QMessageBox.information(self, "Success", f"User '{username}' deleted successfully!")
                self._populate_users_table() # Refresh