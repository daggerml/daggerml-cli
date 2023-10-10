import os
from pathlib import Path

PROFILE = os.getenv('DML_PROFILE', 'DEFAULT')
DML_DIR = Path.joinpath(Path.home(), '.local', 'dml')
REPO_DIR = Path.joinpath(DML_DIR, 'repo')
CONFIG_DIR = Path.joinpath(Path.cwd(), '.dml')
REPO_CONFIG_FILE = Path.joinpath(CONFIG_DIR, 'repo')
HEAD_CONFIG_FILE = Path.joinpath(CONFIG_DIR, 'head')

REPO = None
REPO_PATH = None
HEAD = None

os.makedirs(str(REPO_DIR), mode=0o700, exist_ok=True)

if Path.exists(REPO_CONFIG_FILE):
    with open(REPO_CONFIG_FILE, 'r') as f:
        REPO = f.read().strip()

if REPO:
    REPO_PATH = Path.joinpath(REPO_DIR, REPO)

if Path.exists(HEAD_CONFIG_FILE):
    with open(HEAD_CONFIG_FILE, 'r') as f:
        HEAD = f.read().strip()
