# this is not a setup.py its just a script, sorry.
import os
import platform
system = platform.system()
if system not in ("Windows"):
    python_location = f"#!{os.getcwd()}/venv/bin/python"
    default_save_location = ""
else:
    python_location = f"#!{os.getcwd()}\\venv\\bin\\python"

file = "sync.py"
with open(file) as f:
    lines = f.readlines()
with open(file, "w") as f:
    lines[0] = python_location
    f.writelines(lines)
