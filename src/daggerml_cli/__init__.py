# SPDX-FileCopyrightText: 2023-present Micha Niskin <micha.niskin@gmail.com>
#
# SPDX-License-Identifier: MIT

try:
    from daggerml_cli.__about__ import __version__
except ImportError:
    __version__ = "local"
