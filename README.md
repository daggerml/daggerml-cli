# daggerml-cli [![PyPI - Version](https://img.shields.io/pypi/v/daggerml-cli.svg)](https://pypi.org/project/daggerml-cli) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daggerml-cli.svg)](https://pypi.org/project/daggerml-cli)

![The Prince of DAGness](img/prince-of-dagness.jpg)


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

> [!TIP]
> Shell completion is available for bash/zsh via [argcomplete](https://github.com/kislyuk/argcomplete).


## Test

```sh
hatch run pytest .
```

## Build

```sh
hatch run dml-build pypi
```

## License

`daggerml-cli` is distributed under the terms of the [MIT](LICENSE.txt) license.
