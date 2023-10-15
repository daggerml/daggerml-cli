import os
from dataclasses import dataclass
from getpass import getuser
from socket import gethostname
from daggerml_cli.util import readfile, writefile
from daggerml_cli.repo import Ref


@dataclass
class Config:
    DEBUG: bool = None
    CONFIG_DIR: str = None
    PROJECT_DIR: str = None
    _REPO: str = None
    _HEAD: str = None
    _USER: str = None
    _REPO_PATH: str = None

    @property
    def HEAD(self):
        if self._HEAD is None:
            self._HEAD = readfile(self.PROJECT_DIR, 'head')
        return self._HEAD

    @HEAD.setter
    def HEAD(self, value):
        writefile(value, self.PROJECT_DIR, 'head')
        self._HEAD = value

    @property
    def HEADREF(self):
        return Ref(f'head/{self.HEAD}') if self.HEAD else None

    @property
    def REPO(self):
        if self._REPO is None:
            self._REPO = readfile(self.PROJECT_DIR, 'repo')
        return self._REPO

    @REPO.setter
    def REPO(self, value):
        writefile(value, self.PROJECT_DIR, 'repo')
        self._REPO = value

    @property
    def REPO_DIR(self):
        if self.CONFIG_DIR:
            return os.path.join(self.CONFIG_DIR, 'repo')

    @property
    def REPO_PATH(self):
        if self._REPO_PATH:
            return self._REPO_PATH
        if self.REPO_DIR and self.REPO:
            return os.path.join(self.REPO_DIR, self.REPO)

    @property
    def USER(self):
        if self._USER is None:
            self._USER = readfile(self.CONFIG_DIR, 'config', 'user')
        return self._USER or f'{getuser()}@{gethostname()}'

    @USER.setter
    def USER(self, value):
        writefile(value, self.CONFIG_DIR, 'config', 'user')
        self._USER = value
