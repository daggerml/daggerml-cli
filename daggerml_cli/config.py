import os
from pathlib import Path

DML_DIR = os.getenv('DML_DIR', os.path.join(str(Path.home()), '.local', 'dml'))
REPO_DIR = os.getenv('DML_REPO_DIR', os.path.join(DML_DIR, 'repo'))
PROJECT_DIR = os.getenv('DML_PROJECT_DIR', os.path.join(str(Path.cwd()), '.dml'))
REPO_CONFIG_FILE = os.path.join(PROJECT_DIR, 'repo')
HEAD_CONFIG_FILE = os.path.join(PROJECT_DIR, 'head')

REPO = os.getenv('DML_REPO', None)
REPO_PATH = os.getenv('DML_REPO_PATH', None)
HEAD = os.getenv('DML_BRANCH', None)

os.makedirs(str(REPO_DIR), mode=0o700, exist_ok=True)

if REPO is None and os.path.exists(REPO_CONFIG_FILE):
    with open(REPO_CONFIG_FILE, 'r') as f:
        REPO = f.read().strip()

if REPO and REPO_PATH is None:
    REPO_PATH = os.path.join(REPO_DIR, REPO)

if HEAD is None and os.path.exists(HEAD_CONFIG_FILE):
    with open(HEAD_CONFIG_FILE, 'r') as f:
        HEAD = f.read().strip()
