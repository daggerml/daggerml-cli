# daggerml-cli

[![PyPI - Version](https://img.shields.io/pypi/v/daggerml-cli.svg)](https://pypi.org/project/daggerml-cli)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daggerml-cli.svg)](https://pypi.org/project/daggerml-cli)


## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Test](#test)
- [Build](#build)
- [License](#license)

## Install

```sh
pipx install daggerml-cli
```

## Usage

```sh
dml --help
dml COMMAND --help
dml COMMAND SUBCOMMAND --help
```

## Test

```sh
hatch -e test run pytest .
```

## Build

```sh
hatch -e test run dml-build pypi
```

## License

`daggerml-cli` is distributed under the terms of the [MIT](LICENSE.txt) license.
