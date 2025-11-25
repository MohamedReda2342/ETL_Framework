# main_app.py
import sys
import yaml
import bcrypt
from PySide6.QtCore import Slot
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QDialog, QWidget, QVBoxLayout, 
    QLabel, QLineEdit, QPushButton, QMessageBox, QTabWidget
)

# --- Import your converted pages ---
from comparison_page_widget import ComparisonPageWidget 
from etl_page_widget import ETLPageWidget             
from settings_page_widget import SettingsPageWidget   

# --- Login Dialog (copied from previous response) ---
class LoginDialog(QDialog):
    def __init__(self, config_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Login")
        self.setModal(True)
        self.username = None # Store the username

        try:
            with open(config_path) as file:
                self.config = yaml.safe_load(file)
                self.users = self.config.get('credentials', {}).get('usernames', {})
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not load config file: {e}")
            sys.exit()

        layout = QVBoxLayout(self)
        self.username_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.login_button = QPushButton("Login")
        self.cancel_button = QPushButton("Cancel")
        
        layout.addWidget(QLabel("Username:"))
        layout.addWidget(self.username_input)
        layout.addWidget(QLabel("Password:"))
        layout.addWidget(self.password_input)
        layout.addWidget(self.login_button)
        layout.addWidget(self.cancel_button)
        
        self.login_button.clicked.connect(self.on_login)
        self.cancel_button.clicked.connect(self.reject)

    @Slot()
    def on_login(self):
        username = self.username_input.text()
        password = self.password_input.text().encode('utf-8')

        if not username in self.users:
            QMessageBox.warning(self, "Login Failed", "User not found.")
            return

        hashed_password = self.users[username]['password'].encode('utf-8')
        
        if bcrypt.checkpw(password, hashed_password):
            self.username = username # Save the username
            self.accept()
        else:
            QMessageBox.warning(self, "Login Failed", "Incorrect password.")
            return

# --- Simple Authenticator object for the pages ---
class AppAuthenticator:
    def __init__(self, username, main_app_window):
        self.username = username
        self.main_app = main_app_window

    @Slot()
    def logout(self):
        self.main_app.close()
        QMessageBox.information(self.main_app, "Logged Out", "You have been logged out.")

class MainWindow(QMainWindow):
    def __init__(self, authenticator):
        super().__init__()
        self.authenticator = authenticator
        self.setWindowTitle("ETL and SMX Comparator Tool")
        self.resize(1400, 900)

        # --- Create the Tabbed Interface ---
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # --- Create and add your pages as tabs ---
        self.comparator_page = ComparatorPageWidget(self.authenticator)
        self.etl_page = ETLPageWidget(self.authenticator)
        self.settings_page = SettingsPageWidget(self.authenticator) 
        
        self.tabs.addTab(self.comparator_page, "SMX Comparison")
        self.tabs.addTab(self.etl_page, "ETL scripts generation")
        self.tabs.addTab(self.settings_page, "Settings") 


# --- Application Entry Point ---
def login_and_run_app(app_instance):
    login_dialog = LoginDialog(config_path='config.yaml')
    
    if login_dialog.exec() == QDialog.DialogCode.Accepted:
        main_window = MainWindow(authenticator=None) 
        
        authenticator = AppAuthenticator(username=login_dialog.username, main_app_window=main_window)
        
        # Set the authenticator on the window and ALL pages
        main_window.authenticator = authenticator
        main_window.comparator_page.authenticator = authenticator
        main_window.etl_page.authenticator = authenticator
        main_window.settings_page.authenticator = authenticator # <-- NEW
        
        # Link logout buttons
        main_window.comparator_page.logout_button.clicked.connect(authenticator.logout)
        main_window.etl_page.logout_button.clicked.connect(authenticator.logout)
        
        # The settings page will build itself, including its auth check
        main_window.settings_page.check_permissions() # <-- NEW
        
        main_window.show()
        return True
    else:
        return False


if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    if login_and_run_app(app):
        sys.exit(app.exec())
    else:
        sys.exit()