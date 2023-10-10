import os
from pathlib import Path

PROFILE = os.getenv('DML_PROFILE', 'DEFAULT')
DML_DIR = Path.joinpath(Path.home(), '.local', 'dml')
DB_DIR = Path.joinpath(DML_DIR, 'db')
CONFIG_DIR = Path.joinpath(Path.cwd(), '.dml')
DB_CONFIG_FILE = Path.joinpath(CONFIG_DIR, 'db')
HEAD_CONFIG_FILE = Path.joinpath(CONFIG_DIR, 'head')

DB = None
DB_PATH = None
HEAD = None

os.makedirs(str(DB_DIR), mode=0o700, exist_ok=True)

if Path.exists(DB_CONFIG_FILE):
    with open(DB_CONFIG_FILE, 'r') as f:
        DB = f.read().strip()

if DB:
    DB_PATH = Path.joinpath(DB_DIR, DB)

if Path.exists(HEAD_CONFIG_FILE):
    with open(HEAD_CONFIG_FILE, 'r') as f:
        HEAD = f.read().strip()
