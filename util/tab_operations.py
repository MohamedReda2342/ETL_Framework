import streamlit as st
from util import df_utlis

# Tab options with their key types
tab_options = {
    "stream": ["STREAM"],
    "stg tables": ["create_stg_table_and_view"],
    "bkey": ["All","REG_BKEY", "REG_BKEY_DOMAIN", "REG_BKEY_PROCESS", "bkey_views", "BKEY_CALL"],
    "bmap": ["All","REG_BMAP", "REG_BMAP_DOMAIN", "Insert BMAP values", "Create LKP views"],

    "srci": ["create_SCRI_table", "create_SCRI_view", "create_SCRI_input_view", "EXEC_SRCI"],
    "core tables": ["create_core_table", "create_core_table_view", "CORE_KEY_COL_REG", "create_core_input_view","HIST_REG"]
}


# Field requirements for each key type (action)
field_requirements = {
    # Environment is required for all actions
    "environment": "all",
    
    # Data type is only required for REG_BKEY
    "data_type": ["REG_BKEY"],
    
    # Key set is required for these actions
    "key_set": [
        "STREAM", "BKEY_CALL", "REG_BKEY", "REG_BKEY_PROCESS", 
        "REG_BKEY_DOMAIN", "bkey_views","all_bkey"
    ],
    
    # Key domain is required for these actions
    "key_domain": [
        "STREAM", "BKEY_CALL", "REG_BKEY_PROCESS", "REG_BKEY_DOMAIN", 
        "bkey_views","all_bkey"
    ],
    
    # STG tables is required for these actions
    "stg_tables": [
        "STREAM", "BKEY_CALL", "bkey_views", "REG_BKEY_PROCESS", 
        "create_stg_table_and_view", "EXEC_SRCI", "create_SCRI_table",
        "create_SCRI_view", "create_SCRI_input_view","all_bkey"
    ],
    
    # Code set names is required for these actions
    "code_set_names": ["REG_BMAP", "REG_BMAP_DOMAIN", "Insert BMAP values", "Create LKP views","all_bmap"],
    
    # Code domain names is required for these actions
    "code_domain_names": ["REG_BMAP_DOMAIN", "Insert BMAP values","all_bmap"],
    
    # Core tables is required when tab is "6- core tables"
    "core_tables": ["tab_6_core","HIST_REG","create_core_table","create_core_table_view","CORE_KEY_COL_REG"],
    
    # Mapping names is required for these actions
    "mapping_names": ["create_core_input_view"]

}

def get_action_options(tab_name):
    """Get available actions for a specific tab"""
    return tab_options.get(tab_name, [])

def get_key_type_options():
    """Get all available tabs (key areas)"""
    return list(tab_options.keys())




def is_field_required(field_name, selected_action, selected_tab=None):
    """Check if a field is required for the given action"""
    requirements = field_requirements.get(field_name, [])
    
    if requirements == "all":
        return True
    elif requirements == "tab_6_core":
        return selected_tab == "6- core tables"
    elif isinstance(requirements, list):
        return selected_action in requirements
    
    return False

def get_disable_status(field_name, selected_action, selected_tab=None):
    """Get disable status for a field (opposite of required)"""
    return not is_field_required(field_name, selected_action, selected_tab)

def validate_all_required_fields(selected_action, selected_tab, **field_values):
    """
    Validate all required fields for the selected action
    Returns: (is_valid, missing_fields_message)
    """
    missing_fields = []
    
    # Map of field names to user-friendly names
    field_display_names = {
        "selected_environment": "Environment",
        "selected_data_type": "Data Type Flag",
        "selected_key_set": "Key Set Name",
        "selected_domains": "Key Domain",
        "selected_tables": "STG Table",
        "selected_code_set_names": "Code Set Name",
        "selected_code_domain_names": "Code Domain Name",
        "selected_core_table": "Core Table",
        "selected_mapping_name": "Mapping Name"
    }
    
    # Map field values to requirement names
    field_mapping = {
        "environment": field_values.get("selected_environment"),
        "data_type": field_values.get("selected_data_type"),
        "key_set": field_values.get("selected_key_set"),
        "key_domain": field_values.get("selected_domains"),
        "stg_tables": field_values.get("stg_table_options") if field_values.get("select_all_stg_tables") else field_values.get("selected_tables"),
        "code_set_names": field_values.get("selected_code_set_names"),
        "code_domain_names": field_values.get("selected_code_domain_names"),
        "core_tables": field_values.get("selected_core_table"),
        "mapping_names": field_values.get("selected_mapping_name")
    }
    
    # Check each field
    for field_req_name, field_value in field_mapping.items():
        if is_field_required(field_req_name, selected_action, selected_tab):
            # Check if field has a valid value
            if not field_value or (isinstance(field_value, list) and not any(field_value)):
                # Get the corresponding display name
                for field_var_name, display_name in field_display_names.items():
                    if field_req_name in field_var_name.lower() or field_var_name.lower() in field_req_name:
                        missing_fields.append(display_name)
                        break
                else:
                    missing_fields.append(field_req_name.replace("_", " ").title())
    
    # Generate user-friendly message
    if missing_fields:
        if len(missing_fields) == 1:
            message = f"Please select: {missing_fields[0]}"
        elif len(missing_fields) == 2:
            message = f"Please select: {missing_fields[0]} and {missing_fields[1]}"
        else:
            message = f"Please select: {', '.join(missing_fields[:-1])}, and {missing_fields[-1]}"
        return False, message
    
    return True, "All required fields are completed"

def get_all_disable_statuses(selected_action, selected_tab):
    """Get all disable statuses for form fields"""
    return {
        "disable_frequency": True,  # Currently no actions require frequency
        "disable_data_type": get_disable_status("data_type", selected_action),
        "disable_key_set": get_disable_status("key_set", selected_action),
        "disable_key_domain": get_disable_status("key_domain", selected_action),
        "disable_stg_tables": get_disable_status("stg_tables", selected_action),
        "disable_code_set_names": get_disable_status("code_set_names", selected_action),
        "disable_code_domain_names": get_disable_status("code_domain_names", selected_action),
        "disable_core_tables": get_disable_status("core_tables", selected_action, selected_tab),
        "disable_mapping_names": get_disable_status("mapping_names", selected_action)
    }