import os
import contextlib

def log_to_file(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    return contextlib.redirect_stdout(open(log_path, "w"))
