# etl_page_widget.py
import sys
import os
import pandas as pd
import re
from datetime import datetime

# Import PySide components
from PySide6.QtCore import Slot, Qt
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFileDialog,
    QComboBox, QListWidget, QCheckBox, QPlainTextEdit, QMessageBox,
    QApplication, QGridLayout, QListWidgetItem
)

# Import your existing backend logic
# These .py files can now be compiled by Nuitka!
import generating_scripts
import util.df_utlis as df_utlis
import util.tab_operations as tab_operations
import util.Queries as Queries
# (We will handle auth in main_app.py)
from PySide6.QtGui import QSyntaxHighlighter, QTextCharFormat, QColor, QFont
from pygments import lexers, styles
from pygments.lexers import SqlLexer
from pygments.styles import get_style_by_name

class SQLHighlighter(QSyntaxHighlighter):
    def __init__(self, document):
        super().__init__(document)
        # 1. Setup the Pygments Lexer (SQL) and Style (Monokai)
        self.lexer = SqlLexer()
        self.style = get_style_by_name("monokai")
        self.formats = {}
        
        # 2. Pre-calculate the styling for every token type
        for token, style_def in self.style:
            text_format = QTextCharFormat()
            if style_def['color']:
                text_format.setForeground(QColor(f"#{style_def['color']}"))
            if style_def['bgcolor']:
                text_format.setBackground(QColor(f"#{style_def['bgcolor']}"))
            if style_def['bold']:
                text_format.setFontWeight(QFont.Bold)
            if style_def['italic']:
                text_format.setFontItalic(True)
            self.formats[token] = text_format

    def highlightBlock(self, text):
        """Called automatically whenever the text changes"""
        # 3. Ask Pygments to tokenize the text
        for index, token_type, value in self.lexer.get_tokens_unprocessed(text):
            # 4. Apply the formatting to the editor
            while token_type not in self.formats:
                token_type = token_type.parent
            if token_type in self.formats:
                self.setFormat(index, len(value), self.formats[token_type])

class CheckableComboBox(QComboBox):
    """A custom QComboBox that supports multiple checkable items."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setEditable(True)
        self.lineEdit().setReadOnly(True)
        
        # Use a QListWidget as the view
        self.list_widget = QListWidget(self)
        self.setView(self.list_widget)
        self.setModel(self.list_widget.model())

        # --- FIXED SIGNAL CONNECTIONS ---
        # Only connect itemChanged. Connecting itemClicked causes a "Double Toggle" 
        # bug where checking a box immediately unchecks it.
        self.list_widget.itemChanged.connect(self._on_item_changed)
        
        self._allow_item_changed_signal = True
        self._placeholder = "Choose tables..."

    def setPlaceholderText(self, text):
        self._placeholder = text
        self.lineEdit().setText(text)

    def _on_item_changed(self, item):
        """Updates text when a checkbox is toggled."""
        if self._allow_item_changed_signal:
            self._update_text()

    def _update_text(self):
        checked_items = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                checked_items.append(item.text())
        
        if not checked_items:
            self.lineEdit().setText(self._placeholder)
        else:
            self.lineEdit().setText(", ".join(checked_items))

    def addItems(self, texts):
        self._allow_item_changed_signal = False
        self.list_widget.clear()
        for text in texts:
            item = QListWidgetItem(text)
            item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
            item.setCheckState(Qt.CheckState.Unchecked)
            self.list_widget.addItem(item)
        self._allow_item_changed_signal = True
        self._update_text()

    def get_selected_items(self):
        checked_items = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                checked_items.append(item.text())
        return checked_items

    def select_all(self, checked=True):
        self._allow_item_changed_signal = False
        state = Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(state)
        self._allow_item_changed_signal = True
        self._update_text()
        
    def clear(self):
        self._allow_item_changed_signal = False
        self.list_widget.clear()
        self._allow_item_changed_signal = True
        self._update_text()


class ETLPageWidget(QWidget):
    def __init__(self, authenticator, parent=None):
        super().__init__(parent)
        self.authenticator = authenticator
        
        # Store SMX data
        self.smx_model = {}
        self.excel_file_name = ""

        self._init_ui()
        self._connect_signals()
        self._update_all_disables() # Start with all widgets disabled

    def _init_ui(self):
        # This replaces st.columns
        main_layout = QHBoxLayout(self)
        # --- 1. Sidebar ---
        sidebar_widget = self._create_sidebar()
        # --- 2. Main Panel ---
        main_panel_widget = self._create_main_panel()
        
        main_layout.addWidget(sidebar_widget, 1) # 1-part stretch
        main_layout.addWidget(main_panel_widget, 4) # 4-parts stretch

    def _create_sidebar(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        widget.setObjectName("sidebar")
        widget.setStyleSheet("#sidebar { background-color: #f0f2f6; }")

        self.uploader_button = QPushButton("Upload SMX File")
        self.uploader_label = QLabel("No file loaded.")
        self.logout_button = QPushButton("Logout")

        layout.addWidget(self.uploader_button)
        layout.addWidget(self.uploader_label)
        layout.addStretch()
        layout.addWidget(self.logout_button)
        
        return widget

    def _create_main_panel(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # --- Row 1: Env, Key Area, Key Type ---
        row1_layout = QHBoxLayout()
        self.combo_environment = QComboBox()
        self.combo_environment.addItems(["", "TST", "DEV", "PROD"])
        
        # --- FIX IS HERE ---
        self.combo_key_area = QComboBox()
        self.combo_key_area.addItems([""] + tab_operations.get_key_type_options()) # Added blank
        self.combo_key_type = QComboBox()
        self.combo_key_type.addItems([""]) # Added blank
        # --- END OF FIX ---
        
        row1_layout.addWidget(QLabel("Environment:"))
        row1_layout.addWidget(self.combo_environment)
        row1_layout.addWidget(QLabel("Key Area:"))
        row1_layout.addWidget(self.combo_key_area)
        row1_layout.addWidget(QLabel("Key Type:"))
        row1_layout.addWidget(self.combo_key_type)
        layout.addLayout(row1_layout)

        # --- Row 2: Frequency, Data Type ---
        row2_layout = QHBoxLayout()
        self.combo_frequency = QComboBox()
        self.combo_frequency.addItems(["", "Daily", "weekly", "Monthly"])
        self.combo_data_type = QComboBox()
        self.combo_data_type.addItems(["", "int", "BIG int"])
        
        row2_layout.addWidget(QLabel("Frequency:"))
        row2_layout.addWidget(self.combo_frequency)
        row2_layout.addWidget(QLabel("Data Type Flag:"))
        row2_layout.addWidget(self.combo_data_type)
        layout.addLayout(row2_layout)

        # --- Row 3: Key Set, Key Domain ---
        row3_layout = QHBoxLayout()
        self.combo_key_set = QComboBox()
        self.combo_key_domain = QComboBox()
        row3_layout.addWidget(QLabel("Key Set Name:"))
        row3_layout.addWidget(self.combo_key_set)
        row3_layout.addWidget(QLabel("Key Domain:"))
        row3_layout.addWidget(self.combo_key_domain)
        layout.addLayout(row3_layout)

        # --- Row 4: STG Tables ---
        row4_layout = QHBoxLayout()
        self.combo_stg_tables = CheckableComboBox()
        self.check_all_stg = QCheckBox("Select all tables")
        
        row4_layout.addWidget(QLabel("STG Table:"))
        row4_layout.addWidget(self.combo_stg_tables, 2) # Give 3-part stretch
        row4_layout.addWidget(self.check_all_stg, 1) # Give 1-part stretch
        layout.addLayout(row4_layout)
        
        # --- Row 5: Code Set, Code Domain ---
        row5_layout = QHBoxLayout()
        self.combo_code_set = QComboBox()
        self.combo_code_domain = QComboBox()
        row5_layout.addWidget(QLabel("Code Set Name:"))
        row5_layout.addWidget(self.combo_code_set)
        row5_layout.addWidget(QLabel("Code Domain Name:"))
        row5_layout.addWidget(self.combo_code_domain)
        layout.addLayout(row5_layout)

        # --- Row 6: Core Table, Mapping Name ---
        row6_layout = QHBoxLayout()
        self.combo_core_table = QComboBox()
        self.combo_mapping_name = QComboBox()
        row6_layout.addWidget(QLabel("Core Table:"))
        row6_layout.addWidget(self.combo_core_table)
        row6_layout.addWidget(QLabel("Mapping Name:"))
        row6_layout.addWidget(self.combo_mapping_name)
        layout.addLayout(row6_layout)

        # --- Row 7: Action Buttons ---
        row7_layout = QHBoxLayout()
        self.btn_generate_query = QPushButton("Generate Query")
        self.btn_export_query = QPushButton("Export Query")
        self.btn_execute_query = QPushButton("Execute Query")
        
        row7_layout.addWidget(self.btn_generate_query)
        row7_layout.addWidget(self.btn_export_query)
        row7_layout.addWidget(self.btn_execute_query)
        layout.addLayout(row7_layout)

        # --- Row 8: Code Editor ---
        self.query_editor = QPlainTextEdit()
        self.query_editor.setStyleSheet("""
            QPlainTextEdit {
                background-color: #272822; 
                color: #F8F8F2;
                font-family: Consolas, monospace;
                font-size: 14px;
            }
        """)
        self.highlighter = SQLHighlighter(self.query_editor.document())
        self.query_editor.setPlaceholderText("Generated SQL query will appear here...")
        layout.addWidget(self.query_editor)
        
        return widget

    def _connect_signals(self):
        # --- Sidebar ---
        self.uploader_button.clicked.connect(self._on_upload_file)

        # --- Main Panel: Reactive Dropdowns ---
        self.combo_key_area.currentTextChanged.connect(self._on_key_area_changed)
        self.combo_key_type.currentTextChanged.connect(self._on_key_type_changed)
        
        self.combo_key_set.currentTextChanged.connect(self._on_key_set_changed)
        self.combo_key_domain.currentTextChanged.connect(self._on_key_domain_changed)
        
        self.check_all_stg.toggled.connect(self._on_check_all_stg)
        
        self.combo_code_set.currentTextChanged.connect(self._on_code_set_changed)
        
        self.combo_core_table.currentTextChanged.connect(self._on_core_table_changed)

        # --- Main Panel: Action Buttons ---
        self.btn_generate_query.clicked.connect(self._on_generate_query)
        self.btn_export_query.clicked.connect(self._on_export_query)
        self.btn_execute_query.clicked.connect(self._on_execute_query)

    @Slot(str)
    def _on_core_table_changed(self, core_table_name):
        self.combo_mapping_name.clear()
        if 'table mapping' not in self.smx_model or 'core tables' not in self.smx_model:
            return

        all_mapping_names = []
        
        # --- THIS IS THE NEW FIX ---
        # Check if the core table dropdown is even relevant (i.e., enabled)
        if self.combo_core_table.isEnabled():
            # It is relevant, so filter based on its selection
            if core_table_name == "All":
                # Get all core tables (same logic as populate)
                core_tables_options_df = self.smx_model['core tables'][self.smx_model['core tables']['subject area'].str.upper() != 'LKP']
                all_core_tables = core_tables_options_df['table name'].unique().tolist()
                filtered_df = self.smx_model['table mapping'][self.smx_model['table mapping']['target table name'].isin(all_core_tables)]
                all_mapping_names = filtered_df['mapping name'].unique().tolist()
            elif core_table_name == "":
                # No core table selected, so no mappings to show
                self.combo_mapping_name.addItems([""])
                return
            else:
                # A specific core table is selected
                filtered_df = self.smx_model['table mapping'][self.smx_model['table mapping']['target table name'] == core_table_name]
                all_mapping_names = filtered_df['mapping name'].unique().tolist()
        else:
            # The core table dropdown is DISABLED.
            # This means the current action (like REG_CORE_PROCESS)
            # doesn't filter by core table, so we should show ALL mapping names.
            all_mapping_names = self.smx_model['table mapping']['mapping name'].unique().tolist()
        # --- END OF FIX ---
            
        self.combo_mapping_name.addItems(["", "All"] + all_mapping_names)
    
    @Slot()
    def _on_upload_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Upload SMX File", "", "Excel Files (*.xlsx)")
        if not file_path:
            return

        self.uploader_label.setText(os.path.basename(file_path))
        self.excel_file_name = os.path.basename(file_path).split('.')[0]
        
        try:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            
            with open(file_path, 'rb') as f:
                file_content = f.read()

            # Load all sheets
            self.smx_model['bkey'] = df_utlis.load_sheet(file_content, "BKEY")
            self.smx_model['stg tables'] = df_utlis.load_sheet(file_content, "STG tables")
            self.smx_model['stream'] = df_utlis.load_sheet(file_content, "Stream")
            self.smx_model['bmap'] = df_utlis.load_sheet(file_content, "BMAP")
            self.smx_model['bmap values'] = df_utlis.load_sheet(file_content, "BMAP Values")
            self.smx_model['core tables'] = df_utlis.load_sheet(file_content, "CORE tables")
            self.smx_model['table mapping'] = df_utlis.load_sheet(file_content, "Table mapping")
            self.smx_model['column mapping'] = df_utlis.load_sheet(file_content, "Column mapping")



            # Pre-process: lowercase all column names
            for key in self.smx_model:
                self.smx_model[key].columns = [col.lower() for col in self.smx_model[key].columns]

            # Adding alias to column names nefore filtering the stg dataframe 
            # after filteration the columns we need to search for in the natural keys will not exist all of them will be SK & BM
            # so we apply alias before filtering to garuntee that all column will exist
            if 'stg tables' in self.smx_model:
                stg_df = self.smx_model['stg tables']
                # Iterate over unique column names to perform aliasing in natural keys
                for col_name in stg_df['column name stg'].unique():
                    # Match only if col_name is surrounded by spaces or at start/end of string
                    # This pattern ensures no alphanumeric or underscore characters adjacent
                    pattern = r'(?<![A-Za-z0-9_])' + re.escape(str(col_name)) + r'(?![A-Za-z0-9_])'
                    # Create a mask for rows that contain this column name with proper boundaries
                    mask = stg_df['natural key'].astype(str).str.contains(pattern, na=False, regex=True)
                    
                    if mask.any():
                        # Only update rows where the column name is found with proper boundaries
                        stg_df.loc[mask, 'natural key'] = stg_df.loc[mask].apply(
                            lambda row: re.sub(pattern, f"{row['table name stg']}.{col_name}", str(row['natural key'])),
                            axis=1
                        )
                # Update the model
                self.smx_model['stg tables'] = stg_df
            # --- END NEW LOGIC ---

            # After loading, populate the dropdowns
            self._populate_initial_dropdowns()
            self._update_all_disables()
            
            QApplication.restoreOverrideCursor()
            QMessageBox.information(self, "Success", "SMX file loaded successfully.")
            
        except Exception as e:
            QApplication.restoreOverrideCursor()
            QMessageBox.critical(self, "Error", f"Failed to load SMX file: {e}")
            self.smx_model = {} 
            self.uploader_label.setText("No file loaded.")
            self._update_all_disables()

    def _populate_initial_dropdowns(self):
        """Populates all dropdowns with data from the loaded SMX file."""
        if 'bkey' not in self.smx_model:
            return

        # --- Key Set ---
        all_key_set_names = self.smx_model['bkey']['key set name'].replace('', pd.NA).dropna().unique().tolist()
        self.combo_key_set.addItems(["", "All"] + all_key_set_names)
        
        # --- Code Set ---
        all_code_set_names = self.smx_model['bmap']['code set name'].unique().tolist()
        self.combo_code_set.addItems(["", "All"] + all_code_set_names)
        
        # --- Core Table ---
        core_tables_options = self.smx_model['core tables'][self.smx_model['core tables']['subject area'].str.upper() != 'LKP']
        core_tables_options = core_tables_options['table name'].unique().tolist()
        self.combo_core_table.addItems(["", "All"] + core_tables_options)

    @Slot(str)
    def _on_key_area_changed(self, key_area):
        options = tab_operations.get_action_options(key_area)
        self.combo_key_type.clear()
        self.combo_key_type.addItems(options)
        self._update_all_disables()


    @Slot(str)
    def _on_key_type_changed(self, key_type):
        # 1. Reset simple dropdowns to index 0 ("")
        self.combo_frequency.setCurrentIndex(0)
        self.combo_data_type.setCurrentIndex(0)
        
        # 2. Reset "parent" dropdowns to index 0 ("")
        self.combo_key_set.setCurrentIndex(0)
        self.combo_code_set.setCurrentIndex(0)
        
        # 3. Reset STG tables
        self.check_all_stg.setChecked(False) # Uncheck the box
        self.combo_stg_tables.clear()
        if 'stg tables' in self.smx_model:
             all_stg_tables = self.smx_model['stg tables']['table name stg'].dropna().unique().tolist()
             self.combo_stg_tables.addItems(all_stg_tables)
        
        # 4. Handle the special case for core_table
        self.combo_core_table.clear() # Clear it first
        core_tables_options_list = []
        if 'core tables' in self.smx_model:
            if key_type == "HIST_REG":
                core_tables_options = self.smx_model['core tables'][self.smx_model['core tables']['historization key'].isin(['STRT_DT','HIST','END_DT'])]['table name'].unique().tolist()
            else:
                # Default list of core tables
                core_tables_df = self.smx_model['core tables'][self.smx_model['core tables']['subject area'].str.upper() != 'LKP']
                core_tables_options = core_tables_df['table name'].unique().tolist()
            core_tables_options_list.extend(core_tables_options)
        
        # Repopulate and set to index 0
        self.combo_core_table.addItems(["", "All"] + core_tables_options_list)
        
        # --- THIS IS THE NEW FIX ---
        # 5. Manually populate mapping_name if key_type requires it
        #    (because _on_core_table_changed won't be triggered if core_table is disabled)
        self.combo_mapping_name.clear()
        if key_type == "REG_CORE_PROCESS":
            if 'table mapping' in self.smx_model:
                all_mapping_names = self.smx_model['table mapping']['mapping name'].unique().tolist()
                self.combo_mapping_name.addItems(["", "All"] + all_mapping_names)
        else:
            # Let _on_core_table_changed handle it (which it will, by adding a blank)
            self.combo_mapping_name.addItems([""])
        # --- END OF FIX ---

        # 6. Update all widget enabled/disabled states
        self._update_all_disables()
    @Slot(str)
    def _on_key_set_changed(self, key_set_name):
        self.combo_key_domain.clear()
        if 'bkey' not in self.smx_model:
            return

        # --- FIX FOR "All" ---
        all_key_domains = []
        if key_set_name == "All":
            # --- THIS IS THE FIX ---
            # 1. Get all key sets from the 'bkey' sheet (like the Streamlit app did)
            all_key_set_names = self.smx_model['bkey']['key set name'].replace('', pd.NA).dropna().unique().tolist()
            # 2. Filter the 'bkey' sheet by that full list (this is the key step)
            filtered_df = self.smx_model['bkey'][self.smx_model['bkey']['key set name'].isin(all_key_set_names)]
            # 3. Get domains from the filtered result
            all_key_domains = filtered_df['key domain name'].dropna().unique().tolist()
            # --- END OF FIX ---
        elif key_set_name == "":
            self.combo_key_domain.addItems([""])
            self._on_key_domain_changed() # Trigger downstream update
            return
        else:
            # A specific key set is selected (this logic was correct)
            filtered_df = self.smx_model['bkey'][self.smx_model['bkey']['key set name'] == key_set_name]
            all_key_domains = filtered_df['key domain name'].dropna().unique().tolist()
        
        self.combo_key_domain.addItems(["", "All"] + all_key_domains)
        self._on_key_domain_changed() # Trigger downstream update

    @Slot(str)
# In etl_page_widget.py

    
    @Slot(str)
    
    def _on_key_domain_changed(self):
        # This updates STG tables based on Key Set and Key Domain selections
        self.combo_stg_tables.clear()
        if 'stg tables' not in self.smx_model:
            return

        # 1. Start with the full dataframe of STG tables
        # (Note: The Regex aliasing was already applied during upload, so 'natural key' is correct here)
        filtered_stg = self.smx_model['stg tables'].copy()
        
        # 2. Check if filters should apply (based on Enabled state)
        # Match Streamlit Logic: "if disable_key_set and disable_key_domain: stg_table_options = all_stg_tables"
        is_key_set_active = self.combo_key_set.isEnabled()
        is_key_domain_active = self.combo_key_domain.isEnabled()

        if not is_key_set_active and not is_key_domain_active:
            # Filters are disabled, show ALL tables
            pass 
        else:
            # Filters are enabled, we must apply them
            
            # --- Key Set Filter ---
            if is_key_set_active:
                key_set = self.combo_key_set.currentText()
                if not key_set: 
                    # Enabled but empty -> Show Nothing (wait for selection)
                    return 
                elif key_set != "All":
                    # Specific selection -> Filter
                    filtered_stg = filtered_stg[filtered_stg['key set name'] == key_set]
                # If "All" -> Do nothing (keep all rows)

            # --- Key Domain Filter ---
            if is_key_domain_active:
                key_domain = self.combo_key_domain.currentText()
                if not key_domain:
                    # Enabled but empty -> Show Nothing
                    return
                elif key_domain != "All":
                    # Specific selection -> Filter
                    filtered_stg = filtered_stg[filtered_stg['key domain name'] == key_domain]
                # If "All" -> Do nothing (keep all rows)

        # 4. Get the resulting unique table names
        stg_table_options = filtered_stg['table name stg'].dropna().unique().tolist()
        
        self.combo_stg_tables.addItems(stg_table_options)
    @Slot(bool)
    def _on_check_all_stg(self, checked):
        self.combo_stg_tables.select_all(checked)
        self.combo_stg_tables.setEnabled(not checked)

    @Slot(str)
    def _on_code_set_changed(self, code_set_name):
        self.combo_code_domain.clear()
        if 'bmap values' not in self.smx_model or 'bmap' not in self.smx_model:
            return

        all_code_domains = []
        if code_set_name == "All":
            # --- THIS IS THE FIX ---
            # 1. Get all code sets from the 'bmap' sheet
            all_code_set_names = self.smx_model['bmap']['code set name'].unique().tolist()
            # 2. Filter the 'bmap values' sheet by that full list
            filtered_df = self.smx_model['bmap values'][self.smx_model['bmap values']['code set name'].isin(all_code_set_names)]
            # 3. Get domains from the filtered result
            all_code_domains = filtered_df['code domain name'].dropna().unique().tolist()
            # --- END OF FIX ---
        elif code_set_name == "":
            self.combo_code_domain.addItems([""])
            return
        else:
            # This logic was correct
            filtered_df = self.smx_model['bmap values'][self.smx_model['bmap values']['code set name'] == code_set_name]
            all_code_domains = filtered_df['code domain name'].dropna().unique().tolist()
        
        self.combo_code_domain.addItems(["", "All"] + all_code_domains)
    
    def _update_all_disables(self):
        """Central function to enable/disable all widgets based on state."""
        file_loaded = bool(self.smx_model)

        # List all widgets that should be disabled/enabled
        interactive_widgets = [
            self.combo_environment, self.combo_key_area, self.combo_key_type,
            self.combo_frequency, self.combo_data_type, self.combo_key_set,
            self.combo_key_domain, self.combo_stg_tables, self.check_all_stg,
            self.combo_code_set, self.combo_code_domain, self.combo_core_table,
            self.combo_mapping_name, self.btn_generate_query,
            self.btn_export_query, self.btn_execute_query, self.query_editor
        ]

        if not file_loaded:
            # Disable all interactive widgets
            for widget in interactive_widgets:
                widget.setEnabled(False)
            
            # Explicitly ENABLE the upload button and label
            self.uploader_button.setEnabled(True)
            self.uploader_label.setText("No file loaded.")
            return

        # --- File is loaded, enable widgets based on logic ---
        
        # Get disable statuses
        selected_tab = self.combo_key_area.currentText()
        selected_action = self.combo_key_type.currentText()
        
        # --- THIS IS THE NEW FIX ---
        # We must determine the *final* action to correctly check permissions
        final_selected_action = selected_action
        if selected_action == "All" and selected_tab =="bkey":
            final_selected_action = "all_bkey"
        elif selected_action == "All" and selected_tab =="bmap":
            final_selected_action = "all_bmap"
        # --- END OF FIX ---
        
        if not final_selected_action:
            statuses = tab_operations.get_all_disable_statuses(None, None)
        else:
            # Use the *final* action to get the correct disable statuses
            statuses = tab_operations.get_all_disable_statuses(final_selected_action, selected_tab)

        # Set enabled status (opposite of disabled)
        # The top-level combos should ALWAYS be enabled if a file is loaded
        self.combo_environment.setEnabled(True)
        self.combo_key_area.setEnabled(True)
        self.combo_key_type.setEnabled(True)
        
        self.combo_frequency.setEnabled(not statuses["disable_frequency"])
        self.combo_data_type.setEnabled(not statuses["disable_data_type"])
        self.combo_key_set.setEnabled(not statuses["disable_key_set"])
        self.combo_key_domain.setEnabled(not statuses["disable_key_domain"])
        
        stg_disabled = statuses["disable_stg_tables"]
        self.combo_stg_tables.setEnabled(not stg_disabled)
        self.check_all_stg.setEnabled(not stg_disabled)
        if stg_disabled: # If disabled, also uncheck the box
            self.check_all_stg.setChecked(False)

        self.combo_code_set.setEnabled(not statuses["disable_code_set_names"])
        self.combo_code_domain.setEnabled(not statuses["disable_code_domain_names"])
        self.combo_core_table.setEnabled(not statuses["disable_core_tables"])
        self.combo_mapping_name.setEnabled(not statuses["disable_mapping_names"])

        # Enable action buttons and editor
        self.btn_generate_query.setEnabled(True)
        self.btn_export_query.setEnabled(True)
        self.btn_execute_query.setEnabled(True)
        self.query_editor.setEnabled(True)
# --- Main Action Button Slots ---
    def _get_current_state_for_generation(self):
        """Helper to gather all data from UI"""
        
        # --- Get all selections ---
        selected_tab = self.combo_key_area.currentText()
        selected_action = self.combo_key_type.currentText()
        selected_environment = self.combo_environment.currentText()
        selected_data_type = self.combo_data_type.currentText()
        
        # 1. Determine the "real" action *before* validation
        list_of_key_types = []
        final_selected_action = selected_action # This is what we pass to validation
        
        if selected_action == "All" and selected_tab =="bkey":
            final_selected_action = "all_bkey"
            list_of_key_types = tab_operations.get_action_options(selected_tab)
        elif selected_action == "All" and selected_tab =="bmap":
            final_selected_action = "all_bmap"
            list_of_key_types = tab_operations.get_action_options(selected_tab)
            
        # --------------------------------------------------- key set names ------------------------------------------------
        selected_key_set_str = self.combo_key_set.currentText()
        selected_key_set_list = []
        if selected_key_set_str == "All":
            selected_key_set_list = self.smx_model['bkey']['key set name'].replace('', pd.NA).dropna().unique().tolist()
        elif selected_key_set_str:
            selected_key_set_list = [selected_key_set_str]

        # --------------------------------------------------- Key domain name ------------------------------------------------
        selected_domains_str = self.combo_key_domain.currentText()
        selected_domains_list = []
        if selected_domains_str == "All":
            if self.combo_key_set.isEnabled():
                # Filter by selected key sets
                filtered_df = self.smx_model['bkey'][self.smx_model['bkey']['key set name'].isin(selected_key_set_list)]
                selected_domains_list = filtered_df['key domain name'].dropna().unique().tolist()
            else:
                # Key set is disabled, so "All" means ALL domains
                selected_domains_list = self.smx_model['bkey']['key domain name'].dropna().unique().tolist()
        elif selected_domains_str:
            selected_domains_list = [selected_domains_str]

        # --------------------------------------------------- STG tables ------------------------------------------------
        selected_tables_list = self.combo_stg_tables.get_selected_items()
        # --- THIS IS THE FIX (PART 1) ---
        is_all_stg_checked = self.check_all_stg.isChecked()
        # --- END OF FIX ---
        
        # --------------------------------------------------- code_set_names  ------------------------------------------------
        selected_code_set_names_str = self.combo_code_set.currentText()
        selected_code_set_names_list = []
        if selected_code_set_names_str == "All":
            selected_code_set_names_list = self.smx_model['bmap']['code set name'].unique().tolist()
        elif selected_code_set_names_str:
            selected_code_set_names_list = [selected_code_set_names_str]

        # --------------------------------------------------- code_domain_names  ------------------------------------------------
        selected_code_domain_names_str = self.combo_code_domain.currentText()
        selected_code_domain_names_list = []
        if selected_code_domain_names_str == "All":
            if self.combo_code_set.isEnabled():
                # Filter by selected code sets
                filtered_df = self.smx_model['bmap values'][self.smx_model['bmap values']['code set name'].isin(selected_code_set_names_list)]
                selected_code_domain_names_list = filtered_df['code domain name'].dropna().unique().tolist()
            else:
                # Code set is disabled, so "All" means ALL code domains
                selected_code_domain_names_list = self.smx_model['bmap values']['code domain name'].dropna().unique().tolist()
        elif selected_code_domain_names_str:
            selected_code_domain_names_list = [selected_code_domain_names_str]
            
        # ---------------------------------------------------  Core tables  -------------------------------------------------------------------
        selected_core_table_str = self.combo_core_table.currentText()
        selected_core_table_list = []
        if selected_core_table_str == "All":
            if final_selected_action == "HIST_REG": # Use final action
                 core_tables_options = self.smx_model['core tables'][self.smx_model['core tables']['historization key'].isin(['STRT_DT','HIST','END_DT'])]['table name'].unique().tolist()
            else:
                core_tables_options = self.smx_model['core tables'][self.smx_model['core tables']['subject area'].str.upper() != 'LKP']
                core_tables_options = core_tables_options['table name'].unique().tolist()
            selected_core_table_list = core_tables_options
        elif selected_core_table_str:
            selected_core_table_list = [selected_core_table_str]
            
        # ---------------------------------------------------  Mapping Name  -------------------------------------------------------------------
        selected_mapping_name_str = self.combo_mapping_name.currentText()
        selected_mapping_name_list = []
        if selected_mapping_name_str == "All":
            if self.combo_core_table.isEnabled():
                filtered_df = self.smx_model['table mapping'][self.smx_model['table mapping']['target table name'].isin(selected_core_table_list)]
                selected_mapping_name_list = filtered_df['mapping name'].unique().tolist()
            else:
                selected_mapping_name_list = self.smx_model['table mapping']['mapping name'].unique().tolist()
        elif selected_mapping_name_str:
            selected_mapping_name_list = [selected_mapping_name_str]

        # --- Validation ---
        is_valid, validation_message = tab_operations.validate_all_required_fields(
            final_selected_action, selected_tab, # <-- Use the final action
            selected_environment=selected_environment,
            selected_data_type=selected_data_type,
            selected_key_set=selected_key_set_list,
            selected_domains=selected_domains_list,

            selected_tables=selected_tables_list or is_all_stg_checked,
        
            selected_code_set_names=selected_code_set_names_list,
            selected_code_domain_names=selected_code_domain_names_list,
            selected_core_table=selected_core_table_list,
            selected_mapping_name=selected_mapping_name_list
        )
        
        if not is_valid:
            QMessageBox.warning(self, "Validation Error", validation_message)
            return None, None, None, None, None # Return None to signal failure

        # --- Data Type Flag ---
        bigint_flag = 0 if selected_data_type == "int" else 1 if selected_data_type == "BIG int" else None
            
        # --- Filter DataFrames ---
        filtered_smx = self._filter_smx_model(
            selected_key_set_list, selected_domains_list, 
            selected_tables_list, # We pass the list here, not the boolean
            selected_code_set_names_list, selected_code_domain_names_list,
            selected_core_table_list, selected_mapping_name_list, final_selected_action
        )
        
        # Return the final_selected_action, not the original one
        return final_selected_action, selected_environment, bigint_flag, list_of_key_types, filtered_smx
    def _filter_smx_model(self, selected_key_set, selected_domains, selected_tables,
                          selected_code_set_names, selected_code_domain_names,
                          selected_core_table, selected_mapping_name, selected_action):
        """Applies final filters to the SMX model based on selections."""
        
        # Create a copy to avoid modifying the base model
        filtered_smx = self.smx_model.copy()

        # BKey
        if selected_key_set:
            filtered_smx['bkey'] = self.smx_model['bkey'][self.smx_model['bkey']['key set name'].isin(selected_key_set)]
        if selected_domains:
            filtered_smx['bkey'] = filtered_smx['bkey'][filtered_smx['bkey']['key domain name'].isin(selected_domains)]
            print("---->"+filtered_smx['bkey']['key domain name']) 
        # STG 
        if selected_tables:
            stg_df = self.smx_model['stg tables']
            if selected_key_set and selected_domains:
                # Filter by Key Set and Filter by Key Domain
                stg_df = stg_df[stg_df['key set name'].isin(selected_key_set)]
                stg_df = stg_df[stg_df['key domain name'].isin(selected_domains)]
                # Filter by Specific Tables
                stg_df = stg_df[stg_df['table name stg'].isin(selected_tables)]
            filtered_smx['stg tables'] = stg_df
        # BMap
        if selected_code_set_names:
            filtered_smx['bmap'] = self.smx_model['bmap'][self.smx_model['bmap']['code set name'].isin(selected_code_set_names)]
            filtered_smx['bmap values'] = self.smx_model['bmap values'][self.smx_model['bmap values']['code set name'].isin(selected_code_set_names)]
        if selected_code_domain_names:
            filtered_smx['bmap'] = filtered_smx['bmap'][filtered_smx['bmap']['code domain name'].isin(selected_code_domain_names)]
            filtered_smx['bmap values'] = filtered_smx['bmap values'][filtered_smx['bmap values']['code domain name'].isin(selected_code_domain_names)]

        # Core
        if selected_core_table:
            if selected_action == "HIST_REG":
                filtered_smx['core tables'] = self.smx_model['core tables'][self.smx_model['core tables']['historization key'].isin(['STRT_DT','HIST','END_DT'])]
            else:
                filtered_smx['core tables'] = self.smx_model['core tables'][self.smx_model['core tables']['table name'].isin(selected_core_table)]
        
        # Mapping
        if selected_mapping_name:
            filtered_smx['table mapping'] = self.smx_model['table mapping'][self.smx_model['table mapping']['mapping name'].isin(selected_mapping_name)]
            filtered_smx['column mapping'] = self.smx_model['column mapping'][self.smx_model['column mapping']['mapping name'].isin(selected_mapping_name)]

        # Stream
        stg_aliases = filtered_smx['stg tables']['source system alias']
        filtered_stream = self.smx_model['stream'][self.smx_model['stream']['system name'].isin(stg_aliases)]
        stream_name = filtered_stream['stream name'].str.split('_').str[1]
        mask = stream_name.str.lower().apply(lambda x: x in self.combo_key_area.currentText().lower())
        filtered_smx['stream'] = filtered_stream[mask]
        
        return filtered_smx

#-------------------------------------------------------------------------------------------------
    @Slot()
    def _on_generate_query(self):
        try:
            state = self._get_current_state_for_generation()
            if state[0] is None: # Validation failed
                return
                
            selected_action, selected_environment, bigint_flag, list_of_key_types, filtered_smx = state
            
            flattened_queries = []
            if selected_action == "all_bkey" or selected_action == "all_bmap":
                for action in list_of_key_types[1:]:  # Skip "All"
                    script = generating_scripts.main(filtered_smx, action, selected_environment, bigint_flag)
                    flattened_queries.extend(script)
            else:
                script = generating_scripts.main(filtered_smx, selected_action, selected_environment, bigint_flag)
                flattened_queries.extend(script) if isinstance(script, list) else flattened_queries.append(script)
            
            query_for_editor = '\n'.join(df_utlis.flatten_list(flattened_queries))
            self.query_editor.setPlainText(query_for_editor)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to generate query: {e}")

    @Slot()
    def _on_export_query(self):
        query_to_export = self.query_editor.toPlainText()
        if not query_to_export.strip():
            QMessageBox.warning(self, "No Query", "No query to export. Please generate a query first.")
            return

        try:
            # Recreate folder logic from Streamlit
            base_path = "Exported Queries"
            env_folder_path = os.path.join(base_path, self.combo_environment.currentText())
            excel_folder_path = os.path.join(env_folder_path, self.excel_file_name)
            tab_folder_path = os.path.join(excel_folder_path, self.combo_key_area.currentText())
            current_date = datetime.now().strftime("%Y-%m-%d")
            date_folder_path = os.path.join(tab_folder_path, current_date)
            action_folder_path = os.path.join(date_folder_path, self.combo_key_type.currentText())
            
            os.makedirs(action_folder_path, exist_ok=True)
            
            full_timestamp = datetime.now().strftime("%H%M%S")
            # We need the username. We'll get it from the authenticator.
            username = self.authenticator.username if self.authenticator else "unknown_user"
            file_name = f"{full_timestamp}__{username}.sql"
            query_file_path = os.path.join(action_folder_path, file_name)

            with open(query_file_path, "w") as f:
                f.write(query_to_export)
            QMessageBox.information(self, "Success", f"Query exported to: {query_file_path}")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error exporting query: {e}")

    @Slot()
    def _on_execute_query(self):
        query_to_execute = self.query_editor.toPlainText()
        if not query_to_execute.strip():
            QMessageBox.warning(self, "No Query", "No query to execute. Please generate a query first.")
            return
            
        # This is where you would add your database execution logic
        # For now, we just show a success message
        QMessageBox.information(self, "Query Executed", "Query execution logic has not been implemented yet.")
        print(f"Executed: {query_to_execute}")