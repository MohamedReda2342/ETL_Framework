# config.py
import os
import sys
import yaml
from yaml.loader import SafeLoader

def get_app_path(filename: str) -> str:
    """
    Return absolute path for a file in the project root,
    works for script, PyInstaller, and Nuitka.
    """
    if getattr(sys, 'frozen', False):
        # Running from a compiled bundle (PyInstaller or Nuitka)
        # The base_path is the directory of the .exe
        base_path = os.path.dirname(sys.executable)
    else:
        # Running as a normal script.
        # Assume this file is in 'util/', so root is one level up.
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
    return os.path.join(base_path, filename)

def load_config():
    config_path = get_app_path('config.yaml')
    with open(config_path) as file:
        return yaml.load(file, Loader=SafeLoader)

def save_config(config):
    config_path = get_app_path('config.yaml')
    with open(config_path, 'w') as file:
        yaml.dump(config, file, default_flow_style=False)