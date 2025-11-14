import sys
import os
import streamlit.web.cli as stcli

def get_path(filename):
    """Get absolute path whether running as script or bundled (PyInstaller)."""
    if getattr(sys, 'frozen', False):
        # Running from PyInstaller bundle
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, filename)

if __name__ == '__main__':
    app_script_path = get_path("Compare_between_SMX.py")

    sys.argv = [
        "streamlit",
        "run",
        app_script_path,
        "--server.headless=true",
        "--global.developmentMode=false"
    ]
    stcli.main()
