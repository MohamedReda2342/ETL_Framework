import os
import sys
import yaml
from yaml.loader import SafeLoader

def get_app_path(filename: str) -> str:
    """Return absolute path for `filename` in both script and PyInstaller modes.
    Resolves to project root when running as a script.
    """
    if getattr(sys, 'frozen', False):
        # Running from PyInstaller bundle
        base_path = sys._MEIPASS
        return os.path.join(base_path, filename)
    # Running as normal script: project root is parent of util/
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, filename)

def load_config():
    config_path = get_app_path('config.yaml')
    with open(config_path) as file:
        return yaml.load(file, Loader=SafeLoader)

def save_config(config):
    config_path = get_app_path('config.yaml')
    with open(config_path, 'w') as file:
        yaml.dump(config, file, default_flow_style=False)






        