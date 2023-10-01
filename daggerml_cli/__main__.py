# SPDX-FileCopyrightText: 2023-present Micha Niskin <micha.niskin@gmail.com>
#
# SPDX-License-Identifier: MIT
import sys

if __name__ == "__main__":
    from daggerml_cli.cli import daggerml_cli

    sys.exit(daggerml_cli())
